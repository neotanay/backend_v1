#!/bin/bash
set -euo pipefail

# Defaults
AWS_REGION="${AWS_REGION:-us-east-1}"
FUNCTION_NAME="${FUNCTION_NAME:-}"
ENV_FILE="${ENV_FILE:-.env}"

DEPLOY=true
UPDATE_ENV=true

usage() {
    cat <<EOF
Usage:
  ./deploy.sh <function-name> [env-file] [options]

Lambda:
  ./deploy.sh meddra .env.meddra
  ./deploy.sh argus_cpd .env.argus
  ./deploy.sh smart_and_assessment .env.smart
  ./deploy.sh shipment .env.shipment
  ./deploy.sh aggregate .env.aggregate
  ./deploy.sh consumer .env.consumer

Options:
  --build-only   Build package only
  --skip-env     Deploy code but skip environment update
  -h, --help     Show help

Environment Variables:
  AWS_REGION     AWS region (default: us-east-1)
EOF
}

# Parse positional args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-only)
            DEPLOY=false
            shift
            ;;
        --skip-env)
            UPDATE_ENV=false
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            if [ -z "$FUNCTION_NAME" ]; then
                FUNCTION_NAME="$1"
            elif [ "$ENV_FILE" = ".env" ]; then
                ENV_FILE="$1"
            else
                echo "Unknown argument: $1"
                exit 1
            fi
            shift
            ;;
    esac
done

if [ -z "$FUNCTION_NAME" ]; then
    echo "ERROR: Function name is required."
    usage
    exit 1
fi

echo "======================================="
echo "Function : $FUNCTION_NAME"
echo "Region   : $AWS_REGION"
echo "Env File : $ENV_FILE"
echo "======================================="

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

if [ -d "resources" ]; then
    cp -r resources package/
fi

echo "Creating ZIP package..."
powershell.exe -Command \
    "Compress-Archive -Path package\* -DestinationPath function.zip -Force"

echo "Build completed successfully."

if [ "$DEPLOY" = false ]; then
    echo "Skipping deployment (--build-only)."
    exit 0
fi

echo "Deploying code..."

aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --zip-file fileb://function.zip \
    --no-cli-pager

echo "Waiting for code update..."

aws lambda wait function-updated \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION"

if [ "$UPDATE_ENV" = true ]; then

    if [ ! -f "$ENV_FILE" ]; then
        echo "WARNING: $ENV_FILE not found. Skipping environment update."
    else

        echo "Building environment variable list from $ENV_FILE..."

        ENV_VARS=$(
            grep -v '^[[:space:]]*#' "$ENV_FILE" \
            | grep '=' \
            | sed '/^[[:space:]]*$/d' \
            | paste -sd ',' -
        )

        echo "Updating Lambda environment variables..."

        aws lambda update-function-configuration \
            --function-name "$FUNCTION_NAME" \
            --region "$AWS_REGION" \
            --environment "Variables={$ENV_VARS}" \
            --no-cli-pager

        echo "Waiting for configuration update..."

        aws lambda wait function-updated \
            --function-name "$FUNCTION_NAME" \
            --region "$AWS_REGION"
    fi
fi

echo "======================================="
echo "Deployment completed successfully"
echo "Function: $FUNCTION_NAME"
echo "Region  : $AWS_REGION"
echo "======================================="