import os
import json
import time
import boto3

DATABASE = "kms_sdh_analytics"
OUTPUT_LOCATION = "s3://kms-rds-analytics/athena-results/"

TABLE_ALIAS_MAP = {
    "sv_golden_layer": "g",
    "sv_lab_details": "l",
    "sv_product_mapping": "p",
    "sv_med_non_med": "m",
}


def build_column_map():
    athena = boto3.client("athena")

    query = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = '{DATABASE}'
      AND table_name IN (
          'sv_golden_layer',
          'sv_lab_details',
          'sv_product_mapping',
          'sv_med_non_med'
      )
    ORDER BY table_name, ordinal_position
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={
            "OutputLocation": OUTPUT_LOCATION
        }
    )

    query_execution_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {status}")

        time.sleep(2)

    column_map = {}
    next_token = None

    while True:
        params = {
            "QueryExecutionId": query_execution_id
        }

        if next_token:
            params["NextToken"] = next_token

        results = athena.get_query_results(**params)

        rows = results["ResultSet"]["Rows"]

        # Skip header row only on first page
        if not next_token:
            rows = rows[1:]

        for row in rows:
            table_name = row["Data"][0]["VarCharValue"]
            column_name = row["Data"][1]["VarCharValue"]

            alias = TABLE_ALIAS_MAP.get(table_name)

            if alias:
                column_map.setdefault(column_name, []).append({
                    "table": table_name,
                    "alias": alias,
                    "column": column_name
                })

        next_token = results.get("NextToken")

        if not next_token:
            break

    os.makedirs("resources", exist_ok=True)

    output_file = "resources/column_map_alias.json"

    with open(output_file, "w") as f:
        json.dump(column_map, f, indent=2)

    print(f"Saved {len(column_map)} column names to {output_file}")

    return column_map


if __name__ == "__main__":
    build_column_map()