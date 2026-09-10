# Running the application

## Run locally

```bash
python -m venv .venv && source .venv/bin/activate    
pip install -r requirements.txt

cp .env.example .env  
set -a && source .env && set +a

python lambda_handler.py
```

Serves on `http://localhost:8000`. Interactive docs at `/docs`.

Equivalent, with auto-reload while editing:

```bash
uvicorn lambda_handler:app --reload --port 8000
```

### Verify

```bash
curl localhost:8000/health
curl localhost:8000/columns | head -c 300

curl -X POST localhost:8000/filter_multiple_values \
  -H 'Content-Type: application/json' \
  -d '{"current_column_name":"AGE_GROUP",
       "previous_filters":[{"column_name":"COUNTRY","values":["INDIA"]}],
       "limit":50}'


## Deploy to Lambda

```bash
pip install -r requirements.txt -t package/
cp *.py column_map.json column_map_alias.json package/
cd package && zip -r ../function.zip . -x '*__pycache__*' && cd ..

aws lambda update-function-code \
  --function-name argus-cpd-api \
  --zip-file fileb://function.zip
```


### Configuration

```bash
aws lambda update-function-configuration \
  --function-name argus-cpd-api \
  --handler lambda_handler.handler \
  --timeout 30 \
  --memory-size 1024 \
  --environment 'Variables={
      ATHENA_DATABASE=kms_sdh_analytics,
      ATHENA_TABLE=sv_golden_layer,
      ATHENA_OUTPUT_LOCATION=s3://kms-rds-analytics/athena-results/,
      ALLOWED_DOMAIN=https://your-frontend.example.com,
      S3_BUCKET_NAME=argus-cpd-dashboard-web-859217211726,
      AWS_ACCOUNT_ID=<acct>,
      DASHBOARD_ID=<id>,
      QUICKSIGHT_USER_ARN=<arn>
  }'
```

Two things that are easy to get wrong:

- **Handler must be `lambda_handler.handler`.** The old string
  `lambda_handler.lamdba_handler` pointed at a bare FastAPI object, not a
  Mangum adapter.
- **Timeout 30s** to sit just under API Gateway's 29s limit, so you get a
  logged error rather than a silent gateway timeout.



### Smoke test after deploy

```bash
curl https://<api-id>.execute-api.us-east-1.amazonaws.com/prod/health
```

A `200` with `"status":"ok"` means the registry loaded. A cold start error
usually means `column_map.json` failed to load from S3 or is malformed --
check `S3_BUCKET_NAME` and `COLUMN_MAP_KEY`.

