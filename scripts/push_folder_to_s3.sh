#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: AWS_REGION=region $0 <local-folder> <s3-bucket> [s3-folder-prefix]"
  echo
  echo "  local-folder       Path to the local directory to upload"
  echo "  s3-bucket          Destination S3 bucket name (no s3:// prefix)"
  echo "  s3-folder-prefix   Destination folder inside the bucket (default: basename of local-folder)"
  exit 1
}

LOCAL_FOLDER="${1:-}"
BUCKET="${2:-}"
PREFIX="${3:-}"
AWS_REGION="${AWS_REGION:-us-east-1}"

[ -z "$LOCAL_FOLDER" ] && usage
[ -z "$BUCKET" ] && usage

if [ ! -d "$LOCAL_FOLDER" ]; then
  echo "Error: '$LOCAL_FOLDER' is not a directory" >&2
  exit 1
fi

if [ -z "$PREFIX" ]; then
  PREFIX="$(basename "$LOCAL_FOLDER")"
fi
PREFIX="${PREFIX%/}/"

echo "Creating folder s3://$BUCKET/$PREFIX ..."
aws s3api put-object \
  --bucket "$BUCKET" \
  --key "$PREFIX" \
  --region "$AWS_REGION" >/dev/null

echo "Uploading '$LOCAL_FOLDER' -> s3://$BUCKET/$PREFIX ..."
aws s3 cp "$LOCAL_FOLDER" "s3://$BUCKET/$PREFIX" \
  --recursive \
  --region "$AWS_REGION"

echo "Done. Files available at s3://$BUCKET/$PREFIX"
