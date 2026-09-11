#!/bin/bash
set -euo pipefail

FUNCTION_NAME="${FUNCTION_NAME:-argus-cpd-api}"
AWS_REGION="${AWS_REGION:-us-east-1}"
ENV_FILE="${ENV_FILE:-.env}"
DEPLOY=true
UPDATE_ENV=true

for arg in "$@"; do
  case "$arg" in
    --build-only) DEPLOY=false ;;
    --skip-env) UPDATE_ENV=false ;;
    -h|--help)
      echo "Usage: FUNCTION_NAME=name AWS_REGION=region ENV_FILE=path ./run_the_lambda.sh [--build-only] [--skip-env]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

echo "Cleaning package directory..."
rm -rf package function.zip
mkdir -p package

echo "Installing dependencies..."
pip install -r requirements.txt \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -t package/

echo "Copying source files..."
cp *.py package/
cp -r resources package/

echo "Creating ZIP package..."
powershell.exe -Command "Compress-Archive -Path package\* -DestinationPath function.zip -Force"

echo "Build completed successfully."
echo "Output: function.zip"

if [ "$DEPLOY" = false ]; then
  echo "Skipping deploy (--build-only)."
  exit 0
fi

echo "Deploying to Lambda function '$FUNCTION_NAME' in $AWS_REGION..."
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --region "$AWS_REGION" \
  --zip-file fileb://function.zip \
  --no-cli-pager

echo "Waiting for code update to finish..."
aws lambda wait function-updated \
  --function-name "$FUNCTION_NAME" \
  --region "$AWS_REGION"

if [ "$UPDATE_ENV" = true ]; then
  if [ ! -f "$ENV_FILE" ]; then
    echo "Env file '$ENV_FILE' not found; skipping environment variable update." >&2
  else
    echo "Updating environment variables from $ENV_FILE..."
    ENV_JSON_FILE="$(mktemp)"
    python3 -c "
import json, sys

env = {}
with open(sys.argv[1]) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        env[key.strip()] = value.strip()
json.dump({'Variables': env}, sys.stdout)
" "$ENV_FILE" > "$ENV_JSON_FILE"

    aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" \
      --region "$AWS_REGION" \
      --environment "file://$ENV_JSON_FILE" \
      --no-cli-pager

    rm -f "$ENV_JSON_FILE"

    echo "Waiting for configuration update to finish..."
    aws lambda wait function-updated \
      --function-name "$FUNCTION_NAME" \
      --region "$AWS_REGION"
  fi
fi

echo "Deployment completed: $FUNCTION_NAME"
