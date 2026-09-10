from typing import Optional
import os
import hashlib
import json
import socket
from typing import Any, Optional
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError
import logger
import traceback

redis_client: Optional[RedisCluster] = None
REDIS_ENABLED = os.getenv("REDIS_ENABLED", "true").lower() == "true"
ELASTICACHE_ENDPOINT = os.getenv("ELASTICACHE_ENDPOINT", "")
ELASTICACHE_AUTH_TOKEN = os.getenv("ELASTICACHE_AUTH_TOKEN")
REDIS_CONNECT_TIMEOUT_SECONDS = os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", 5)
REDIS_SOCKET_TIMEOUT_SECONDS = os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", 5)
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "quicksight-api")

def get_redis_client() -> Optional[RedisCluster]:
    global redis_client

    if not REDIS_ENABLED or not ELASTICACHE_ENDPOINT:
        print("Redis disabled or ElastiCacheured")
        return None

    redis_client = RedisCluster(
        host=ELASTICACHE_ENDPOINT,
        port=6379,
        password=ELASTICACHE_AUTH_TOKEN or None,
        ssl=True,
        decode_responses=True,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_keepalive=True,
    )

    print(
        "ElastiCache client initialized. endpoint=%s port=%s",
        ELASTICACHE_ENDPOINT,
        6379,
    )

    return redis_client



async def check_redis_connection() -> bool:
    try:
        client = get_redis_client()
        if client is None:
            logger.error("Redis client is None")
            return False
        result = await client.ping()

        logger.info("Redis ping successful: %s", result)
        return True
    except Exception as e:
        logger.error("Redis connection failed: %s", repr(e))
        logger.error(traceback.format_exc())
        return False

def build_cache_key(namespace: str, payload: Any) -> str:
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()
    return f"{REDIS_KEY_PREFIX}:{namespace}:{digest}"


async def cache_get_json(key: str) -> Optional[Any]:
    client = get_redis_client()
    if client is None:     
        return None
    try:
        cached_value = await client.get(key)
        if cached_value is None:
            return None
        return json.loads(cached_value)

    except (
        RedisError,
        RedisConnectionError,
        json.JSONDecodeError,
        TimeoutError,
        socket.timeout,
    ) as exc:
        logger.info(
            "ElastiCache GET failed. key=%s error=%s",
            key,
            repr(exc),
        )
        return None

async def cache_set_json(
    key: str,
    value: Any,
    ttl_seconds: int = 3600,
) -> bool:
    client = get_redis_client()

    if client is None:
        return False

    try:
        await client.set(
            key,
            json.dumps(value),
            ex=ttl_seconds,
        )

        print(f"Redis SET success key={key}")

        return True

    except (
        RedisError,
        RedisConnectionError,
        TimeoutError,
        socket.timeout,
    ) as exc:
        print(f"Redis SET failed. key={key} error={repr(exc)}")
        return False


async def close_redis() -> None:
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
            # print("Redis client closed")
        except Exception as exc:
            print(f"Error closing Redis client: {exc}")
        finally:
            redis_client = None
