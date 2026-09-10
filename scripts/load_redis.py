import asyncio
import json
import time
from typing import List

import athena_filter

from redis_client import (
    build_cache_key,
    cache_set_json,
    close_redis,
)

CACHE_TTL_SECONDS = 86400  # 24 hours


async def warm_column_cache(column_name: str) -> dict:
    try:
        cache_payload = {
            "column": column_name,
            "filters": [],
            "q": "",
            "limit": 50,
            "offset": 0,
        }

        cache_key = build_cache_key(
            "filter-values",
            cache_payload,
        )

        start = time.perf_counter()

        result = await athena_filter.filter_values(
            column=column_name,
            filters=[],
            q="",
            limit=50,
            offset=0,
        )

        await cache_set_json(
            cache_key,
            result,
            ttl_seconds=CACHE_TTL_SECONDS,
        )

        elapsed_ms = round(
            (time.perf_counter() - start) * 1000,
            2,
        )

        print(
            f"Cached column={column_name} "
            f"values={len(result.get('values', []))} "
            f"time={elapsed_ms}ms"
        )

        return {
            "column": column_name,
            "cacheKey": cache_key,
            "status": "SUCCESS",
            "elapsedMs": elapsed_ms,
            "valueCount": len(result.get("values", [])),
        }

    except Exception as exc:
        print(
            f"Failed caching column={column_name}: {str(exc)}"
        )

        return {
            "column": column_name,
            "status": "FAILED",
            "error": str(exc),
        }


async def warm_cache(columns: List[str]):

    # limit concurrent Athena executions
    semaphore = asyncio.Semaphore(10)

    async def run(column):
        async with semaphore:
            return await warm_column_cache(column)

    tasks = [run(column) for column in columns]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=False,
    )

    success = len(
        [r for r in results if r["status"] == "SUCCESS"]
    )

    failed = len(results) - success

    return {
        "totalColumns": len(columns),
        "success": success,
        "failed": failed,
        "results": results,
    }


def lambda_handler(event, context):

    columns = event.get("columns", [])

    if not columns:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": "columns array is required"
                }
            ),
        }

    try:
        result = asyncio.run(
            warm_cache(columns)
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }

    finally:
        try:
            asyncio.run(close_redis())
        except Exception:
            pass