
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from starlette.concurrency import run_in_threadpool

import logger
from column_registry import REGISTRY, _read_local_or_s3

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE")
SV_PRIMARY_TABLE = os.getenv("SV_PRIMARY_TABLE")
ATHENA_OUTPUT_LOCATION = os.getenv("ATHENA_OUTPUT_LOCATION", "")
ATHENA_REGION = os.getenv("ATHENA_REGION", os.getenv("AWS_REGION"))
ATHENA_POLL_INTERVAL_SECONDS = float(os.getenv("ATHENA_POLL_INTERVAL_SECONDS", 2))
ATHENA_QUERY_TIMEOUT_SECONDS = int(os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", 40))
COLUMN_ALIAS_LOCAL_PATH = os.getenv("COLUMN_ALIAS_LOCAL_PATH", os.path.join(_THIS_DIR, "resources", "column_map_alias.json"))
COLUMN_ALIAS_KEY = os.getenv("COLUMN_ALIAS_KEY")
SV_SMART_SEARCH_TABLE = os.getenv("SV_SMART_SEARCH_TABLE", "sv_smart_seach_index")
BASE_ALIAS = "g"
JOIN_TABLES = {
    "l": os.getenv("ATHENA_LAB_DETAILS_TABLE", ""),
    "p": os.getenv("ATHENA_PRODUCT_MAPPING_TABLE", ""),
    "m": os.getenv("ATHENA_MED_NON_MED_TABLE", ""),
}
JOIN_SPECS: Dict[str, Tuple[str, str]] = {
    "l": (BASE_ALIAS, "g.case_id = l.case_id"),
    "p": ("l", "g.prod_cd = p.prod_cd"),
    "m": (BASE_ALIAS, "g.pt_name = m.event_name_med"),
}
for _part in (ATHENA_DATABASE, SV_PRIMARY_TABLE, *JOIN_TABLES.values()):
    if '"' in _part:
        raise RuntimeError(f"Invalid Athena identifier in configuration: {_part!r}")
_FULL_TABLE = f'"{ATHENA_DATABASE}"."{SV_PRIMARY_TABLE}"'

athena_client = boto3.client("athena", region_name=ATHENA_REGION) if ATHENA_REGION else boto3.client("athena")


class AthenaQueryFailed(RuntimeError):
    """The Athena query execution ended in FAILED or CANCELLED state."""


class AthenaQueryTimeout(RuntimeError):
    """The Athena query did not reach a terminal state in time."""

_ALIAS_REF_RE = re.compile(
    r"^(" + "|".join([BASE_ALIAS, *JOIN_TABLES]) + r")\.[a-z][a-z0-9_]{0,127}$"
)


def _load_alias_map() -> Dict[str, str]:
    raw = _read_local_or_s3(COLUMN_ALIAS_LOCAL_PATH, COLUMN_ALIAS_KEY, required=False) or {}
    if not isinstance(raw, dict):
        logger.warn("column_map_alias JSON is not an object; ignoring it")
        return {}

    valid: Dict[str, str] = {}
    rejected: List[str] = []
    for column, candidates in raw.items():
        if not isinstance(column, str) or not isinstance(candidates, list) or not candidates:
            rejected.append(str(column))
            continue

        refs: List[str] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            alias, ref_column = candidate.get("alias"), candidate.get("column")
            if not isinstance(alias, str) or not isinstance(ref_column, str):
                continue
            ref = f"{alias}.{ref_column}"
            if _ALIAS_REF_RE.match(ref):
                refs.append(ref)

        if not refs:
            rejected.append(str(column))
            continue

        chosen = next((r for r in refs if r.startswith(f"{BASE_ALIAS}.")), refs[0])
        valid[column.strip().upper()] = chosen

    if rejected:
        logger.error(
            f"Rejected {len(rejected)} column_map_alias entries with no recognised "
            f"table alias or unsafe column reference: {rejected[:10]}"
        )
    logger.info(f"Loaded {len(valid)} column_map_alias entries for the join layer")
    return valid


ALIAS_BY_COLUMN = _load_alias_map()


def _qualified_column(column: str) -> str:
    """Alias-qualified SQL reference for a registry-resolved column name."""
    ref = ALIAS_BY_COLUMN.get(column)
    if ref:
        return ref
    return f"{BASE_ALIAS}.{REGISTRY.quoted(column)}"


def _required_joins(aliases: set) -> List[Tuple[str, str, str]]:
    needed = set(aliases) - {BASE_ALIAS}
    changed = True
    while changed:
        changed = False
        for alias in list(needed):
            requires, _ = JOIN_SPECS[alias]
            if requires != BASE_ALIAS and requires not in needed:
                needed.add(requires)
                changed = True

    ordered = [a for a in ("l", "p", "m") if a in needed]
    return [(alias, JOIN_TABLES[alias], JOIN_SPECS[alias][1]) for alias in ordered]


def build_query(
    column: str,
    filters: List[Tuple[str, List[str]]],
    q: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[str, List[str]]:
    target = _qualified_column(column)
    aliases_used = {target.split(".", 1)[0]}
    predicates = [f"{target} IS NOT NULL"]
    params: List[str] = []

    for filter_column, values in filters:
        filter_ref = _qualified_column(filter_column)
        aliases_used.add(filter_ref.split(".", 1)[0])
        placeholders = ", ".join("?" for _ in values)
        predicates.append(f"{filter_ref} IN ({placeholders})")
        params.extend(str(v) for v in values)

    if q and q.strip():
        predicates.append(f"UPPER(CAST({target} AS VARCHAR)) LIKE ?")
        params.append(f"%{q.strip().upper()}%")

    where_sql = " AND ".join(predicates)
    fetch_size = int(limit) + 1
    join_sql = "".join(
        f' INNER JOIN "{ATHENA_DATABASE}"."{table}" {alias} ON {on}'
        for alias, table, on in _required_joins(aliases_used)
    )
    sql = (
        f"SELECT DISTINCT {target} AS VAL "
        f"FROM {_FULL_TABLE} {BASE_ALIAS}{join_sql} "
        f"WHERE {where_sql} "
        f"OFFSET {int(max(0, offset))} LIMIT {fetch_size}"
    )
    print("sql", sql, params)
    return sql, params

def _start_and_wait(sql: str, params: List[str]) -> str:
    """Start a query and block (in the calling thread) until it reaches a
    terminal state. Returns the query execution id for fetching results.
    """
    kwargs: Dict[str, Any] = dict(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    if params:
        kwargs["ExecutionParameters"] = params

    response = athena_client.start_query_execution(**kwargs)
    query_execution_id = response["QueryExecutionId"]

    deadline = time.monotonic() + ATHENA_QUERY_TIMEOUT_SECONDS
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return query_execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise AthenaQueryFailed(f"Athena query {state}: {reason}")
        if time.monotonic() > deadline:
            athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise AthenaQueryTimeout(
                f"Athena query {query_execution_id} timed out after {ATHENA_QUERY_TIMEOUT_SECONDS}s"
            )
        time.sleep(ATHENA_POLL_INTERVAL_SECONDS)


def _execute_sync(sql: str, params: List[str]) -> List[Optional[str]]:
    kwargs: Dict[str, Any] = dict(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    if params:
        kwargs["ExecutionParameters"] = params

    response = athena_client.start_query_execution(**kwargs)
    query_execution_id = response["QueryExecutionId"]

    deadline = time.monotonic() + ATHENA_QUERY_TIMEOUT_SECONDS
    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise AthenaQueryFailed(f"Athena query {state}: {reason}")
        if time.monotonic() > deadline:
            athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise AthenaQueryTimeout(
                f"Athena query {query_execution_id} timed out after {ATHENA_QUERY_TIMEOUT_SECONDS}s"
            )
        time.sleep(ATHENA_POLL_INTERVAL_SECONDS)

    rows: List[Optional[str]] = []
    paginator = athena_client.get_paginator("get_query_results")
    first_page = True
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        result_rows = page["ResultSet"]["Rows"]
        if first_page:
            result_rows = result_rows[1:]  # header row echoes the column name
            first_page = False
        for row in result_rows:
            data = row.get("Data", [])
            rows.append(data[0].get("VarCharValue") if data else None)
    return rows

def _execute_search_index_sync(sql: str, params: List[str]) -> List[Dict[str, Optional[str]]]:
    """Same shape as `_execute_sync`, but for a two-column result set
    (matched value, source column) rather than a single column.
    """
    query_execution_id = _start_and_wait(sql, params)

    rows: List[Dict[str, Optional[str]]] = []
    paginator = athena_client.get_paginator("get_query_results")
    first_page = True
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        result_rows = page["ResultSet"]["Rows"]
        if first_page:
            result_rows = result_rows[1:]  # header row echoes the column names
            first_page = False
        for row in result_rows:
            data = row.get("Data", [])
            value = data[0].get("VarCharValue") if len(data) > 0 and data[0] else None
            source_column = data[1].get("VarCharValue") if len(data) > 1 and data[1] else None
            rows.append({"value": value, "source_column": source_column})
    return rows

async def filter_values(
    column: str,
    filters: List[Tuple[str, List[str]]] = None,
    q: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    filters = filters or []

    column = REGISTRY.resolve(column)
    filters = [(REGISTRY.resolve(c), v) for c, v in filters]

    sql, params = build_query(column, filters, q, limit, offset)
    logger.debug("filter_multiple_values Athena SQL", " ".join(sql.split()))

    started = time.monotonic()
    rows = await run_in_threadpool(_execute_sync, sql, params)
    elapsed_ms = round((time.monotonic() - started) * 1000, 1)

    values = [v for v in rows if v is not None]
    has_more = len(values) > limit
    values = values[:limit]

    result = {
        "column": column,
        "values": values,
        "counts": None,
        "offset": offset,
        "limit": limit,
        "hasMore": has_more,
        "nextOffset": offset + limit if has_more else None,
        "source": "athena",
        "elapsedMs": elapsed_ms,
    }
    if elapsed_ms > 1000:
        logger.warn(
            f"Slow Athena filter query: {elapsed_ms}ms column={column} filters={len(filters)}"
        )
    return result

async def search_smart_index(query: str):
    sql = f"""
        WITH filtered AS (
            SELECT
                column_name,
                column_value
            FROM "{ATHENA_DATABASE}"."{SV_SMART_SEARCH_TABLE}"
            WHERE LOWER(COALESCE(column_value, '')) LIKE ?
            LIMIT 1000
        )
        SELECT
            column_name,
            json_format(CAST(array_agg(column_value) AS JSON)) AS matches,
            count(*) AS count
        FROM filtered
        GROUP BY column_name
        ORDER BY count DESC
        LIMIT 10
    """

    params = [f"%{query.lower()}%"]
    logger.info("smart search Athena SQL", sql)
    started = time.monotonic()
    rows = await run_in_threadpool(
        _execute_search_index_sync,
        sql,
        params,
    )
    elapsed_ms = round((time.monotonic() - started) * 1000, 1)
    if elapsed_ms > 1000:
        logger.warn(f"Slow smart-search Athena query: {elapsed_ms}ms query={query!r}")

    logger.info("smart search returned %d columns for query=%s",len(rows), query)
    return rows