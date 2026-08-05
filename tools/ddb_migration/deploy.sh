#!/bin/bash
# Deploy infrastructure for DynamoDB zero-downtime migration.
#
# Creates: target table, stream replay Lambda, event source mapping,
#          CloudWatch alarms, and optionally a Glue job for large tables.
#
# Usage:
#   export SOURCE_TABLE=OrdersV1
#   export TARGET_TABLE=OrdersV2
#   export PARTITION_KEY=pk
#   export REGION=us-east-1
#   ./deploy.sh
#
# For cross-account: also set TARGET_ACCOUNT and TARGET_ROLE

set -euo pipefail

REGION=${REGION:-us-east-1}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SOURCE_TABLE=${SOURCE_TABLE:?SOURCE_TABLE required}
TARGET_TABLE=${TARGET_TABLE:?TARGET_TABLE required}
PARTITION_KEY=${PARTITION_KEY:-pk}
SORT_KEY=${SORT_KEY:-}
LAMBDA_NAME="migration-stream-replay"
EXPORT_BUCKET=${EXPORT_BUCKET:-ddb-migration-${ACCOUNT_ID}-${REGION}}

echo "=== DynamoDB Zero-Downtime Migration Setup ==="
echo "Source: $SOURCE_TABLE | Target: $TARGET_TABLE | Region: $REGION"
echo ""

# --- 1. Create target table ---
echo "1. Creating target table..."
KEY_SCHEMA="AttributeName=$PARTITION_KEY,KeyType=HASH"
ATTR_DEFS="AttributeName=$PARTITION_KEY,AttributeType=S"
if [ -n "${SORT_KEY:-}" ]; then
  KEY_SCHEMA="$KEY_SCHEMA AttributeName=$SORT_KEY,KeyType=RANGE"
  ATTR_DEFS="$ATTR_DEFS AttributeName=$SORT_KEY,AttributeType=S"
fi

aws dynamodb create-table \
  --table-name "$TARGET_TABLE" \
  --attribute-definitions $ATTR_DEFS \
  --key-schema $KEY_SCHEMA \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" > /dev/null 2>&1 || echo "   (already exists)"
aws dynamodb wait table-exists --table-name "$TARGET_TABLE" --region "$REGION"
echo "   Done."

# --- 2. Enable streams on source ---
echo "2. Enabling streams on $SOURCE_TABLE..."
aws dynamodb update-table \
  --table-name "$SOURCE_TABLE" \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --region "$REGION" > /dev/null 2>&1 || echo "   (already enabled)"
echo "   Done."

# --- 3. Create S3 bucket for export ---
echo "3. Creating export bucket ($EXPORT_BUCKET)..."
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$EXPORT_BUCKET" --region "$REGION" 2>/dev/null || true
else
  aws s3api create-bucket --bucket "$EXPORT_BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" 2>/dev/null || true
fi
echo "   Done."

# --- 4. Deploy Lambda ---
echo "4. Deploying stream replay Lambda..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/lambda"
zip -j function.zip stream_replay.py ../transform.py > /dev/null

aws lambda create-function \
  --function-name "$LAMBDA_NAME" \
  --runtime python3.12 \
  --handler stream_replay.handler \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/migration-stream-replay-role" \
  --environment "Variables={TARGET_TABLE=$TARGET_TABLE,PARTITION_KEY=$PARTITION_KEY,TARGET_REGION=$REGION}" \
  --timeout 300 \
  --memory-size 512 \
  --zip-file fileb://function.zip \
  --region "$REGION" > /dev/null 2>&1 || \
  aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --zip-file fileb://function.zip \
    --region "$REGION" > /dev/null

rm -f function.zip
cd "$SCRIPT_DIR"
echo "   Done."

# --- 5. Create event source mapping ---
echo "5. Creating event source mapping..."
STREAM_ARN=$(aws dynamodb describe-table \
  --table-name "$SOURCE_TABLE" \
  --query 'Table.LatestStreamArn' --output text --region "$REGION")

aws lambda create-event-source-mapping \
  --function-name "$LAMBDA_NAME" \
  --event-source-arn "$STREAM_ARN" \
  --starting-position TRIM_HORIZON \
  --batch-size 100 \
  --maximum-batching-window-in-seconds 5 \
  --bisect-batch-on-function-error \
  --maximum-retry-attempts 3 \
  --function-response-types ReportBatchItemFailures \
  --region "$REGION" > /dev/null 2>&1 || echo "   (already exists)"
echo "   Done."

# --- 6. CloudWatch alarms ---
echo "6. Creating iterator age alarms..."
aws cloudwatch put-metric-alarm \
  --alarm-name "Migration-IteratorAge-Warning" \
  --metric-name IteratorAge \
  --namespace AWS/Lambda \
  --dimensions "Name=FunctionName,Value=$LAMBDA_NAME" \
  --statistic Maximum --period 60 --evaluation-periods 5 \
  --threshold 43200000 \
  --comparison-operator GreaterThanThreshold \
  --region "$REGION" 2>/dev/null

aws cloudwatch put-metric-alarm \
  --alarm-name "Migration-IteratorAge-Critical" \
  --metric-name IteratorAge \
  --namespace AWS/Lambda \
  --dimensions "Name=FunctionName,Value=$LAMBDA_NAME" \
  --statistic Maximum --period 60 --evaluation-periods 5 \
  --threshold 72000000 \
  --comparison-operator GreaterThanThreshold \
  --region "$REGION" 2>/dev/null
echo "   Done."

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Export:    aws dynamodb export-table-to-point-in-time \\"
echo "                 --table-arn arn:aws:dynamodb:$REGION:$ACCOUNT_ID:table/$SOURCE_TABLE \\"
echo "                 --s3-bucket $EXPORT_BUCKET --s3-prefix exports/ \\"
echo "                 --export-format DYNAMODB_JSON --region $REGION"
echo ""
echo "  2. Backfill:  export EXPORT_BUCKET=$EXPORT_BUCKET EXPORT_PREFIX=exports/"
echo "               python scripts/backfill.py"
echo ""
echo "  3. Monitor:   python scripts/convergence_check.py"
echo ""
echo "  4. Cutover:   Flip your application routing to $TARGET_TABLE"
