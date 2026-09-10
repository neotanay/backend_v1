"""
FastAPI application for the Argus CPD dashboard filter API.

Deployment note: set the Lambda handler to `lambda_handler.handler`.
The previous entry point (`lamdba_handler = asyncio.run(create_app())`) was a
misspelling bound to a FastAPI instance rather than a Mangum adapter, and the
module could not import at all because the `@app.on_event("startup")` block
below it referenced an undefined name and had an empty body.
"""
import json
import os
import time
import re
import uuid
from typing import Any, Optional

from contextlib import asynccontextmanager
import boto3
from datetime import datetime, UTC
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum
from starlette.concurrency import run_in_threadpool

import athena_filter
import logger
from column_registry import REGISTRY, UnknownColumn
from models import FilterValuesRequest
from column_registry import _read_local_or_s3
from redis_client import build_cache_key, cache_get_json, check_redis_connection, cache_set_json, close_redis
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))


MIN_SEARCH_LENGTH = int(os.getenv("MIN_SEARCH_LENGTH", "2"))
SMART_SEARCH_FILE = os.getenv("SMART_SEARCH_FILE", "")
LOCAL_SMART_SEARCH_FILE = os.getenv("COLUMN_MAP_PATH", os.path.join(_THIS_DIR, "resources", "smart_search_data.json"))
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
QUICKSIGHT_REGION = os.getenv("QUICKSIGHT_REGION", "us-east-1")
DASHBOARD_ID = os.getenv("DASHBOARD_ID")
QUICKSIGHT_USER_ARN = os.getenv("QUICKSIGHT_USER_ARN")
ALLOWED_DOMAIN = os.getenv("ALLOWED_DOMAIN", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
BOOKMARKS_PREFIX = os.getenv("BOOKMARKS_PREFIX", "bookmarks/")
QS_SESSION_LIFETIME_MINUTES = int(os.getenv("QS_SESSION_LIFETIME_MINUTES", 600))
DEFAULT_BOOKMARK_NAME = os.environ.get("DEFAULT_BOOKMARK_NAME", "Untitled bookmark")
TEXT_INPUT_COLUMNS = [c.strip() for c in os.getenv("TEXT_INPUT_COLUMNS", "").split(",") if c.strip()]
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))
HIGH_CARDINALITY_THRESHOLD = int(os.getenv("HIGH_CARDINALITY_THRESHOLD", 100))
ALL_QS_FEATURES = {
    "statePersistence": "StatePersistence",
    "bookmarks": "Bookmarks",
    "sharedView": "SharedView",
    "schedules": "Schedules",
    "recentSnapshots": "RecentSnapshots",
    "thresholdAlerts": "ThresholdAlerts",
}
_qs_features_env = os.getenv("QS_FEATURES")
ENABLED_QS_FEATURES = (
    {f.strip() for f in _qs_features_env.split(",") if f.strip()}
    if _qs_features_env is not None
    else set(ALL_QS_FEATURES)
)

s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", QUICKSIGHT_REGION))
qs_client = boto3.client("quicksight", region_name=QUICKSIGHT_REGION)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_redis()

def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if ALLOWED_DOMAIN == "*" else [ALLOWED_DOMAIN],
        allow_methods=["GET", "POST", "OPTIONS", "DELETE", "PUT"],
        allow_headers=["Content-Type", "authorization"],
    )

    @app.exception_handler(UnknownColumn)
    async def unknown_column_handler(request: Request, exc: UnknownColumn):
        logger.warn("Rejected unknown column", repr(exc))
        return JSONResponse(
            status_code=400,
            content={"error": "Unknown column name", "type": "UnknownColumn"},
        )

    @app.exception_handler(athena_filter.AthenaQueryFailed)
    async def athena_query_failed_handler(request: Request, exc: athena_filter.AthenaQueryFailed):
        logger.error("Athena query failed", repr(exc))
        return JSONResponse(
            status_code=503,
            content={
                "error": "Query engine is unavailable. Please retry shortly.",
                "type": "AthenaQueryFailed",
            },
        )

    @app.exception_handler(athena_filter.AthenaQueryTimeout)
    async def athena_query_timeout_handler(request: Request, exc: athena_filter.AthenaQueryTimeout):
        logger.error("Athena query timed out", repr(exc))
        return JSONResponse(
            status_code=503,
            content={
                "error": "Query timed out. Try narrowing your filters.",
                "type": "AthenaQueryTimeout",
            },
        )

    @app.exception_handler(ClientError)
    async def aws_client_error_handler(request: Request, exc: ClientError):
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        message = exc.response.get("Error", {}).get("Message", str(exc))
        logger.error("AWS client error", code, message)
        return JSONResponse(status_code=502, content={"error": message, "type": code})

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled error", repr(exc))
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "type": "InternalError"},
        )

    @app.get("/health")
    async def health():
        return {
            "status": "ok",
            "registry": REGISTRY.diagnostics(),
            "check_redis_connection" : await check_redis_connection()
        }

    @app.get("/config")
    async def get_config():
        return {
            "app": {"name": "Argus CPD Dashboard", "subtitle": "Powered by Amazon QuickSight"},
            "textInputColumns": TEXT_INPUT_COLUMNS,
            "highCardinalityThreshold": HIGH_CARDINALITY_THRESHOLD,
            "minSearchLength": MIN_SEARCH_LENGTH,
        }

    @app.get("/columns")
    async def handle_columns():
        return {
            "columns": REGISTRY.columns,
            "paramMap": REGISTRY.param_map(),
            "paramMapFull": REGISTRY.param_map_full(),
            "filterGroupColumns": sorted(REGISTRY.filter_group_columns),
        }

    @app.get("/columns/describe")
    async def describe_columns():
        """Column metadata: data type, cardinality, tier, filterability."""
        return {"columns": REGISTRY.describe()}

    @app.get("/search")
    async def get_column_data(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100)
    ):
        query = str(request.query_params.get("query", "")).strip().lower()
        if len(query) < 3:
            raise HTTPException(
                status_code=400,
                detail="Query must contain at least 3 characters."
            )
        rows = await athena_filter.search_smart_index(query)
        results = []
        for row in rows:
            column_name = row.get("value")
            if isinstance(row.get("source_column"), str):
                row["source_column"] = json.loads(row["source_column"])
            matches = row["source_column"]
            results.append(
                {
                    "column": column_name,
                    # "paramName": param_name,
                    "matches": matches[:10] or [],
                    "count": row.get("count", len(matches))
                }
            )

        total_count = len(results)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_results = results[start:end]
        logger.info(
            'search "%s": %d columns matched',
            query,
            total_count
        )

        return {
            "query": query,
            "results": paginated_results,
            "pagination": {
                "page": page,
                "pageSize": page_size,
                "totalCount": total_count,
                "totalPages": (
                    (total_count + page_size - 1) // page_size
                    if total_count > 0
                    else 0
                ),
                "hasNext": end < total_count,
                "hasPrevious": page > 1
            },
            "debug": {
                "columnsMatched": total_count
            }
        }

    @app.post("/filter_multiple_values")
    async def filter_multiple_values(req: FilterValuesRequest):
        cache_payload = {
            "column": req.current_column_name,
            "filters": sorted(
                [
                    {
                        "column_name": f.column_name,
                        "values": sorted(f.values),
                    }
                    for f in req.previous_filters
                ],
                key=lambda item: item["column_name"],
            ),
            "q": (req.q or "").strip().lower(),
            "limit": req.limit,
            "offset": req.offset,
        }
        cache_key = build_cache_key("filter-values", cache_payload)
        start = time.perf_counter()
        cached_result = await cache_get_json(cache_key) 
        cache_ms = round((time.perf_counter() - start) * 1000, 2) 
        if cached_result is not None:
            cached_result["source"] = "cache"
            cached_result["elapsedMs"] = cache_ms
            cached_result["cache"] = {
                "hit": True,
                "key": cache_key,
            }
            logger.info("filter_multiple_values cache hit ""column=%s filters=%d", 
                        req.current_column_name,len(req.previous_filters))
            return cached_result      
        result = await athena_filter.filter_values(
            column=req.current_column_name,
            filters=[(f.column_name, f.values) for f in req.previous_filters],
            q=req.q,
            limit=req.limit,
            offset=req.offset,
        )
            # CACHE SET
        await cache_set_json(cache_key, result, ttl_seconds= CACHE_TTL_SECONDS)
        result["cache"] = {
                "hit": False,
                "key": cache_key,
            }
        logger.info(
            f'filter_multiple_values column={result["column"]} '
            f'filters={len(req.previous_filters)} source={result["source"]} '
            f'{result["elapsedMs"]}ms: {len(result["values"])} values'
        )
        return result   

    @app.post("/bookmark")
    async def post_bookmark(request: Request):
        if not S3_BUCKET_NAME:
            raise HTTPException(status_code=500, detail="S3_BUCKET_NAME is not configured")
        try:
            body = await request.json()
        except Exception:
            body = None

        if not isinstance(body, dict):
            return JSONResponse(status_code=400, content={"error": "Invalid body"})
        
        encrypted = body.get("encrypted")
        name = body.get("name").strip() or DEFAULT_BOOKMARK_NAME
        if not encrypted:
            return JSONResponse(status_code=400, content={"error": "No data"})
        created_at = datetime.now(UTC).isoformat()
        bookmark_id = uuid.uuid4().hex[:12]
        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        await run_in_threadpool(
            s3_client.put_object,
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps({"encrypted": encrypted, "name": name, "iv": body.get("iv"), "createdAt": created_at}),
            ContentType="application/json",
        )
        logger.info(f"Bookmark saved: {bookmark_id}")
        return {"id": bookmark_id, "name": name, "createdAt": created_at}

    @app.get("/bookmark")
    async def get_bookmark(request: Request):
        if not S3_BUCKET_NAME:
            raise HTTPException(status_code=500, detail="S3_BUCKET_NAME is not configured")

        bookmark_id = str(request.query_params.get("id", ""))
        if not bookmark_id:
            return JSONResponse(status_code=400, content={"error": "No id"})
        if not re.fullmatch(r"[A-Za-z0-9]{1,64}", bookmark_id):
            return JSONResponse(status_code=400, content={"error": "Invalid id"})

        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        try:
            obj = await run_in_threadpool(
                s3_client.get_object, Bucket=S3_BUCKET_NAME, Key=key
            )
            body_bytes = await run_in_threadpool(obj["Body"].read)
            return json.loads(body_bytes.decode("utf-8"))
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchKey" or status == 404:
                return JSONResponse(status_code=404, content={"error": "Bookmark not found"})
            raise

    @app.put("/bookmark")
    async def rename_bookmark(request: Request):
        id = str(request.query_params.get("id", ""))
        if not re.fullmatch(r"[A-Za-z0-9]+", id):
            raise HTTPException(status_code=400, detail="Invalid id")

        body = await request.json()
        new_name = (body.get("name") or "").strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="No name provided")
        key = f"{BOOKMARKS_PREFIX}{id}.json"

        try:
            obj = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
            )
            data = json.loads(obj["Body"].read())

        except ClientError as err:
            error_code = err.response.get("Error", {}).get("Code")

            if error_code in ("NoSuchKey", "404"):
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "Bookmark not found",
                        "debug": {
                            "s3Bucket": S3_BUCKET_NAME,
                            "s3Key": key,
                        },
                    },
                )

            raise

        data["name"] = new_name
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json",
        )

        logger.info("Bookmark renamed: %s -> %s", id, new_name)

        return {
            "id": id,
            "name": new_name,
        }

    @app.delete("/bookmark")
    async def delete_bookmark(request: Request):
        id = str(request.query_params.get("id", ""))
        if not re.fullmatch(r"[A-Za-z0-9]+", id):
            raise HTTPException(status_code=400, detail="Invalid id")
        key = f"{BOOKMARKS_PREFIX}{id}.json"
        try:
            s3_client.delete_object(
                Bucket=S3_BUCKET_NAME,
                Key=key
            ) 
        except ClientError as exc:
            logger.exception("Failed to delete bookmark %s", id)
            raise HTTPException(
                status_code=500,
                detail="Failed to delete bookmark"
            ) from exc

        logger.info("Bookmark deleted: %s", id)

        return {
            "deleted": True,
            "id": id
        }
    
    @app.get("/bookmarks")
    async def get_bookmarks(
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100)
    ):
        bookmarks = []

        try:
            paginator = s3_client.get_paginator("list_objects_v2")

            for s3_page in paginator.paginate(
                Bucket=S3_BUCKET_NAME,
                Prefix=BOOKMARKS_PREFIX
            ):
                for obj_summary in s3_page.get("Contents", []):
                    key = obj_summary["Key"]

                    if not key.endswith(".json"):
                        continue

                    bookmark_id = key[len(BOOKMARKS_PREFIX):-len(".json")]

                    try:
                        obj = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=key
                        )
                        data = json.loads(obj["Body"].read())
                    except Exception:
                        logger.exception(
                            "Skipping unreadable bookmark %s", key
                        )
                        continue

                    bookmarks.append({
                        "id": bookmark_id,
                        "name": data.get("name") or DEFAULT_BOOKMARK_NAME,
                        "createdAt": data.get("createdAt")
                            or obj_summary["LastModified"].isoformat(),
                    })

        except ClientError:
            logger.exception("Failed to list bookmarks")
            raise

        # Sort newest first
        bookmarks.sort(key=lambda b: b["createdAt"], reverse=True)

        # Pagination
        total_count = len(bookmarks)
        start = (page - 1) * page_size
        end = start + page_size

        paginated_bookmarks = bookmarks[start:end]

        return JSONResponse(
            status_code=200,
            content={
                "bookmarks": paginated_bookmarks,
                "pagination": {
                    "page": page,
                    "pageSize": page_size,
                    "totalCount": total_count,
                    "totalPages": (total_count + page_size - 1) // page_size,
                    "hasNext": end < total_count,
                    "hasPrevious": page > 1,
                },
            },
        )

    @app.get("/")
    async def get_root():
        missing = [
            name
            for name, value in (
                ("AWS_ACCOUNT_ID", AWS_ACCOUNT_ID),
                ("DASHBOARD_ID", DASHBOARD_ID),
                ("QUICKSIGHT_USER_ARN", QUICKSIGHT_USER_ARN),
            )
            if not value
        ]
        if missing:
            logger.error("QuickSight configuration missing", ", ".join(missing))
            return JSONResponse(
                status_code=500,
                content={
                    "error": f"Missing configuration: {', '.join(missing)}",
                    "type": "ConfigError",
                },
            )

        feature_configurations = {
            api_name: {"Enabled": key in ENABLED_QS_FEATURES}
            for key, api_name in ALL_QS_FEATURES.items()
        }

        response = await run_in_threadpool(
            qs_client.generate_embed_url_for_registered_user,
            AwsAccountId=AWS_ACCOUNT_ID,
            UserArn=QUICKSIGHT_USER_ARN,
            SessionLifetimeInMinutes=QS_SESSION_LIFETIME_MINUTES,
            AllowedDomains=[ALLOWED_DOMAIN] if ALLOWED_DOMAIN else [],
            ExperienceConfiguration={
                "Dashboard": {
                    "InitialDashboardId": DASHBOARD_ID,
                    "FeatureConfigurations": feature_configurations,
                }
            },
        )
        return {"embedUrl": response["EmbedUrl"]}

    return app

app = create_app()
handler = Mangum(app, lifespan="auto")


# if __name__ == "__main__":
#     import uvicorn

#     port = int(os.getenv("PORT", "8000"))
    
#     logger.info(f"Server running on http://localhost:{port}")
#     uvicorn.run(app, host="127.0.0.1", port=port)
