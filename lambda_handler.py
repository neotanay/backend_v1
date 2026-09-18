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

# ── Fixed org-member directory ────────────────────────────────────────────
# Hardcoded per request (not pulled from a directory service). Powers the
# "search your name" identity picker and the "share with these people"
# checklist in the frontend. `email` is the value actually stored as a
# bookmark's `owner` and inside `sharedWith` — treat it as a stable id, not
# just a display string.
ORG_MEMBERS: list[dict] = [
    {"name": "Sachin Aggarwal", "email": "saggar03@kenvue.com"},
    {"name": "Aniket Agrawal", "email": "aagraw08@kenvue.com"},
    # >>> Placeholder — replace with the real dashboard owner's name/email <<<
    {"name": "John Doe", "email": "jdoe@kenvue.com"},
]
_ORG_MEMBER_EMAILS: set[str] = {m["email"].strip().lower() for m in ORG_MEMBERS}

# ── Dashboard owner(s) ─────────────────────────────────────────────────────
# The only identity/identities allowed to approve or reject a "Submit for
# Community" request (see post_bookmark_community_decide below). A set
# supports more than one owner without an API shape change.
# >>> REPLACE "jdoe@kenvue.com" with the real owner email(s) — this is a
# placeholder for John Doe above, not a real address <<<
DASHBOARD_OWNERS: set[str] = {"jdoe@kenvue.com"}


def _is_dashboard_owner(email: str) -> bool:
    return bool(email) and email.strip().lower() in DASHBOARD_OWNERS


def get_org_members_with_owner_flag() -> list[dict]:
    return [{**m, "isOwner": _is_dashboard_owner(m["email"])} for m in ORG_MEMBERS]


def _resolve_viewer_identity(request: Request) -> str:
    return (request.query_params.get("viewer") or "").strip().lower()


def _resolve_owner_identity(body: dict) -> str:
    return (body.get("owner") or "").strip().lower()

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
        # NEW: see _resolve_owner_identity() above — this is the one line
        # that changes when real SSO replaces the client-supplied "owner".
        owner = _resolve_owner_identity(body)
        if not encrypted:
            return JSONResponse(status_code=400, content={"error": "No data"})
        created_at = datetime.now(UTC).isoformat()
        bookmark_id = uuid.uuid4().hex[:12]
        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        await run_in_threadpool(
            s3_client.put_object,
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps({
                "encrypted": encrypted,
                "name": name,
                "iv": body.get("iv"),
                "createdAt": created_at,
                "owner": owner,
                "visibility": "private",
                "sharedWith": [],
                "communityStatus": "none",
            }),
            ContentType="application/json",
        )
        logger.info(f"Bookmark saved: {bookmark_id}")
        return {
            "id": bookmark_id, "name": name, "createdAt": created_at,
            "owner": owner, "visibility": "private", "sharedWith": [],
            "communityStatus": "none",
        }

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

    @app.post("/bookmark/share")
    async def post_bookmark_share(request: Request):
        try:
            body = await request.json()
        except Exception:
            body = None
        if not isinstance(body, dict):
            return JSONResponse(status_code=400, content={"error": "Invalid body"})

        bookmark_id = str(body.get("id") or "").strip()
        if not bookmark_id or not re.fullmatch(r"[A-Za-z0-9]+", bookmark_id):
            return JSONResponse(status_code=400, content={"error": "Invalid bookmark id"})

        visibility = str(body.get("visibility") or "").strip().lower()
        if visibility not in ("public", "private"):
            return JSONResponse(status_code=400, content={"error": "visibility must be 'public' or 'private'"})

        shared_with_raw = body.get("sharedWith") or []
        if not isinstance(shared_with_raw, list):
            return JSONResponse(status_code=400, content={"error": "sharedWith must be a list"})
        shared_with = sorted({
            e.strip().lower() for e in shared_with_raw
            if isinstance(e, str) and e.strip().lower() in _ORG_MEMBER_EMAILS
        })

        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        try:
            obj = await run_in_threadpool(s3_client.get_object, Bucket=S3_BUCKET_NAME, Key=key)
            data = json.loads(await run_in_threadpool(obj["Body"].read))
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchKey" or status == 404:
                return JSONResponse(status_code=404, content={"error": "Bookmark not found"})
            raise

        data["visibility"] = visibility
        data["sharedWith"] = shared_with if visibility == "private" else []
        await run_in_threadpool(
            s3_client.put_object, Bucket=S3_BUCKET_NAME, Key=key,
            Body=json.dumps(data), ContentType="application/json",
        )

        logger.info("Bookmark %s sharing updated: visibility=%s sharedWith=%s", bookmark_id, visibility, data["sharedWith"])
        return {"id": bookmark_id, "visibility": data["visibility"], "sharedWith": data["sharedWith"]}

    @app.post("/bookmark/community/submit")
    async def post_bookmark_community_submit(request: Request):
        try:
            body = await request.json()
        except Exception:
            body = None
        if not isinstance(body, dict):
            return JSONResponse(status_code=400, content={"error": "Invalid body"})

        bookmark_id = str(body.get("id") or "").strip()
        if not bookmark_id or not re.fullmatch(r"[A-Za-z0-9]+", bookmark_id):
            return JSONResponse(status_code=400, content={"error": "Invalid bookmark id"})
        requester = str(body.get("requester") or "").strip().lower()

        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        try:
            obj = await run_in_threadpool(s3_client.get_object, Bucket=S3_BUCKET_NAME, Key=key)
            data = json.loads(await run_in_threadpool(obj["Body"].read))
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchKey" or status == 404:
                return JSONResponse(status_code=404, content={"error": "Bookmark not found"})
            raise

        owner = (data.get("owner") or "").strip().lower()
        if not requester or requester != owner:
            return JSONResponse(status_code=403, content={"error": "Only the bookmark's owner can submit it for community review"})

        data["communityStatus"] = "pending"
        await run_in_threadpool(
            s3_client.put_object, Bucket=S3_BUCKET_NAME, Key=key,
            Body=json.dumps(data), ContentType="application/json",
        )
        logger.info("Bookmark %s submitted for community review by %s", bookmark_id, requester)
        return {"id": bookmark_id, "communityStatus": "pending"}

    @app.post("/bookmark/community/decide")
    async def post_bookmark_community_decide(request: Request):
        try:
            body = await request.json()
        except Exception:
            body = None
        if not isinstance(body, dict):
            return JSONResponse(status_code=400, content={"error": "Invalid body"})

        bookmark_id = str(body.get("id") or "").strip()
        if not bookmark_id or not re.fullmatch(r"[A-Za-z0-9]+", bookmark_id):
            return JSONResponse(status_code=400, content={"error": "Invalid bookmark id"})

        decision = str(body.get("decision") or "").strip().lower()
        if decision not in ("approve", "reject"):
            return JSONResponse(status_code=400, content={"error": "decision must be 'approve' or 'reject'"})

        reviewer = str(body.get("reviewer") or "").strip().lower()
        if not _is_dashboard_owner(reviewer):
            return JSONResponse(status_code=403, content={"error": "Only the safety-view owner can approve or reject a community submission"})

        key = f"{BOOKMARKS_PREFIX}{bookmark_id}.json"
        try:
            obj = await run_in_threadpool(s3_client.get_object, Bucket=S3_BUCKET_NAME, Key=key)
            data = json.loads(await run_in_threadpool(obj["Body"].read))
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchKey" or status == 404:
                return JSONResponse(status_code=404, content={"error": "Bookmark not found"})
            raise

        if data.get("communityStatus") != "pending":
            return JSONResponse(status_code=409, content={"error": "This bookmark is not awaiting review"})

        if decision == "approve":
            data["visibility"] = "public"
            data["communityStatus"] = "approved"
        else:
            data["communityStatus"] = "rejected"

        await run_in_threadpool(
            s3_client.put_object, Bucket=S3_BUCKET_NAME, Key=key,
            Body=json.dumps(data), ContentType="application/json",
        )
        logger.info("Bookmark %s community decision by %s: %s", bookmark_id, reviewer, decision)
        return {"id": bookmark_id, "visibility": data["visibility"], "communityStatus": data["communityStatus"]}

    @app.get("/org-members")
    async def get_org_members():
        return {"members": get_org_members_with_owner_flag()}

    @app.get("/bookmarks")
    async def get_bookmarks(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100)
    ):
        viewer = _resolve_viewer_identity(request)
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

                    visibility = data.get("visibility") or "public"
                    owner = (data.get("owner") or "").strip().lower()
                    shared_with = {e.strip().lower() for e in (data.get("sharedWith") or [])}
                    community_status = data.get("communityStatus") or "none"

                    visible = (
                        visibility == "public"
                        or (viewer and viewer == owner)
                        or (viewer and viewer in shared_with)
                        or (community_status == "pending" and _is_dashboard_owner(viewer))
                    )
                    if not visible:
                        continue

                    bookmarks.append({
                        "id": bookmark_id,
                        "name": data.get("name") or DEFAULT_BOOKMARK_NAME,
                        "createdAt": data.get("createdAt")
                            or obj_summary["LastModified"].isoformat(),
                        "owner": data.get("owner") or "",
                        "visibility": visibility,
                        "sharedWith": sorted(shared_with),
                        "communityStatus": community_status,
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
lambda_handler = Mangum(app, lifespan="auto")


# if __name__ == "__main__":
#     import uvicorn

#     port = int(os.getenv("PORT", "8000"))
    
#     logger.info(f"Server running on http://localhost:{port}")
#     uvicorn.run(app, host="127.0.0.1", port=port)
