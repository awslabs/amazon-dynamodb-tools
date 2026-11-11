#!/usr/bin/env bash
# Provisions the DynamoDB zero-downtime migration infrastructure.
#
# Idempotent: safe to re-run. Reads ./config.env if present, otherwise relies on
# environment variables. All AWS API calls use the AWS_PROFILE / AWS_REGION
# already in your shell.
#
# Required env vars:
#   SOURCE_TABLE         Existing source table.
#   TARGET_TABLE         Target table to create (or already exists).
#
# Optional env vars (with defaults):
#   PARTITION_KEY=pk
#   PARTITION_KEY_TYPE=S            # S | N | B
#   SORT_KEY=                       # leave empty for hash-only schema
#   SORT_KEY_TYPE=S
#   REGION=us-east-1
#   EXPORT_BUCKET=ddb-migration-<account>-<region>
#   LAMBDA_FUNCTION_NAME=ddb-migration-stream-replay
#   LAMBDA_ROLE_NAME=ddb-migration-stream-replay-role
#   DLQ_NAME=ddb-migration-dlq
#   SNS_TOPIC_NAME=ddb-migration-alerts
#   ITERATOR_AGE_WARN_MS=43200000   # 12h
#   ITERATOR_AGE_CRIT_MS=72000000   # 20h
#
# Cross-account (target table in a different account):
#   TARGET_ACCOUNT=123456789012     # account that owns TARGET_TABLE
#   TARGET_ROLE_ARN=arn:aws:iam::123456789012:role/ddb-migration-target-writer
#                                   # role the Lambda assumes to write to target
#                                   # (must be created in the target account first)
#
# After running this script, the Lambda is wired to the source-table stream and
# replay starts immediately. Subscribe to the printed SNS topic ARN to receive
# IteratorAge alarm notifications.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/config.env" ]]; then
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/config.env"
fi

: "${SOURCE_TABLE:?SOURCE_TABLE is required}"
: "${TARGET_TABLE:?TARGET_TABLE is required}"

PARTITION_KEY="${PARTITION_KEY:-pk}"
PARTITION_KEY_TYPE="${PARTITION_KEY_TYPE:-S}"
SORT_KEY="${SORT_KEY:-}"
SORT_KEY_TYPE="${SORT_KEY_TYPE:-S}"
REGION="${REGION:-us-east-1}"
LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-ddb-migration-stream-replay}"
LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-ddb-migration-stream-replay-role}"
DLQ_NAME="${DLQ_NAME:-ddb-migration-dlq}"
SNS_TOPIC_NAME="${SNS_TOPIC_NAME:-ddb-migration-alerts}"
ITERATOR_AGE_WARN_MS="${ITERATOR_AGE_WARN_MS:-43200000}"
ITERATOR_AGE_CRIT_MS="${ITERATOR_AGE_CRIT_MS:-72000000}"

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
EXPORT_BUCKET="${EXPORT_BUCKET:-ddb-migration-${ACCOUNT}-${REGION}}"

CROSS_ACCOUNT="false"
if [[ -n "${TARGET_ACCOUNT:-}" && "$TARGET_ACCOUNT" != "$ACCOUNT" ]]; then
    CROSS_ACCOUNT="true"
    : "${TARGET_ROLE_ARN:?TARGET_ROLE_ARN is required for cross-account migrations}"
fi

log() { printf "\n[deploy] %s\n" "$*"; }

#
# 1. Target table
#
if [[ "$CROSS_ACCOUNT" == "true" ]]; then
    log "Cross-account mode: skipping target table creation (must exist in account $TARGET_ACCOUNT)"
else
    log "Ensuring target table $TARGET_TABLE exists in $REGION"
    if aws dynamodb describe-table --table-name "$TARGET_TABLE" --region "$REGION" >/dev/null 2>&1; then
        log "  already exists"
    else
        attr_defs="AttributeName=$PARTITION_KEY,AttributeType=$PARTITION_KEY_TYPE"
        key_schema="AttributeName=$PARTITION_KEY,KeyType=HASH"
        if [[ -n "$SORT_KEY" ]]; then
            attr_defs="$attr_defs AttributeName=$SORT_KEY,AttributeType=$SORT_KEY_TYPE"
            key_schema="$key_schema AttributeName=$SORT_KEY,KeyType=RANGE"
        fi
        # shellcheck disable=SC2086
        aws dynamodb create-table \
            --table-name "$TARGET_TABLE" \
            --attribute-definitions $attr_defs \
            --key-schema $key_schema \
            --billing-mode PAY_PER_REQUEST \
            --region "$REGION" >/dev/null
        aws dynamodb wait table-exists --table-name "$TARGET_TABLE" --region "$REGION"
        log "  created"
    fi
    log "Enabling TTL on target table (_ttl attribute) for tombstone expiration"
    aws dynamodb update-time-to-live \
        --table-name "$TARGET_TABLE" \
        --time-to-live-specification "Enabled=true,AttributeName=_ttl" \
        --region "$REGION" >/dev/null 2>&1 || true
fi

#
# 2. Streams on source
#
log "Enabling DynamoDB Streams on source table $SOURCE_TABLE"
aws dynamodb update-table \
    --table-name "$SOURCE_TABLE" \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --region "$REGION" >/dev/null 2>&1 || log "  (already enabled or other error)"
SOURCE_STREAM_ARN="$(aws dynamodb describe-table --table-name "$SOURCE_TABLE" --region "$REGION" --query 'Table.LatestStreamArn' --output text)"
log "  stream ARN: $SOURCE_STREAM_ARN"

#
# 3. S3 export bucket
#
log "Ensuring export bucket s3://$EXPORT_BUCKET exists"
if aws s3api head-bucket --bucket "$EXPORT_BUCKET" --region "$REGION" 2>/dev/null; then
    log "  already exists"
else
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$EXPORT_BUCKET" --region "$REGION" >/dev/null
    else
        aws s3api create-bucket \
            --bucket "$EXPORT_BUCKET" \
            --region "$REGION" \
            --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    fi
    aws s3api put-bucket-encryption \
        --bucket "$EXPORT_BUCKET" \
        --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
    log "  created"
fi

#
# 4. SNS topic for alarm actions
#
log "Ensuring SNS topic $SNS_TOPIC_NAME exists"
SNS_TOPIC_ARN="$(aws sns create-topic --name "$SNS_TOPIC_NAME" --region "$REGION" --query 'TopicArn' --output text)"
log "  topic ARN: $SNS_TOPIC_ARN"

#
# 5. SQS DLQ for stream-replay failures
#
log "Ensuring SQS DLQ $DLQ_NAME exists"
DLQ_URL="$(aws sqs create-queue --queue-name "$DLQ_NAME" --region "$REGION" --query 'QueueUrl' --output text)"
DLQ_ARN="$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --region "$REGION" --query 'Attributes.QueueArn' --output text)"
log "  DLQ URL: $DLQ_URL"
log "  DLQ ARN: $DLQ_ARN"

#
# 6. IAM role for the Lambda
#
log "Ensuring IAM role $LAMBDA_ROLE_NAME exists"
trust_policy=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
)
if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" >/dev/null 2>&1; then
    log "  already exists"
else
    aws iam create-role \
        --role-name "$LAMBDA_ROLE_NAME" \
        --assume-role-policy-document "$trust_policy" >/dev/null
    log "  created; waiting for IAM propagation..."
    sleep 10
fi

inline_policy=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSourceStream",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeStream",
        "dynamodb:GetShardIterator",
        "dynamodb:GetRecords",
        "dynamodb:ListStreams"
      ],
      "Resource": "$SOURCE_STREAM_ARN"
    },
    {
      "Sid": "WriteTarget",
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:$REGION:${TARGET_ACCOUNT:-$ACCOUNT}:table/$TARGET_TABLE"
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:$REGION:$ACCOUNT:log-group:/aws/lambda/$LAMBDA_FUNCTION_NAME:*"
    },
    {
      "Sid": "Dlq",
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "$DLQ_ARN"
    }$( [[ "$CROSS_ACCOUNT" == "true" ]] && cat <<XJSON
,
    {
      "Sid": "AssumeTargetRole",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "$TARGET_ROLE_ARN"
    }
XJSON
    )
  ]
}
JSON
)
aws iam put-role-policy \
    --role-name "$LAMBDA_ROLE_NAME" \
    --policy-name "ddb-migration-stream-replay-inline" \
    --policy-document "$inline_policy" >/dev/null
ROLE_ARN="$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text)"
log "  role ARN: $ROLE_ARN"

#
# 7. Package + deploy Lambda
#
log "Packaging Lambda"
PKG_DIR="$(mktemp -d)"
cp "$SCRIPT_DIR/lambda/stream_replay.py" "$PKG_DIR/"
cp "$SCRIPT_DIR/transform.py" "$PKG_DIR/"
( cd "$PKG_DIR" && zip -q -r "$SCRIPT_DIR/lambda.zip" . )
rm -rf "$PKG_DIR"

env_vars="Variables={TARGET_TABLE=$TARGET_TABLE,PARTITION_KEY=$PARTITION_KEY,TARGET_REGION=$REGION"
if [[ "$CROSS_ACCOUNT" == "true" ]]; then
    env_vars="${env_vars},TARGET_ROLE_ARN=$TARGET_ROLE_ARN"
fi
env_vars="${env_vars}}"

if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$REGION" >/dev/null 2>&1; then
    log "Updating Lambda code"
    aws lambda update-function-code \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --zip-file "fileb://$SCRIPT_DIR/lambda.zip" \
        --region "$REGION" >/dev/null
    aws lambda wait function-updated --function-name "$LAMBDA_FUNCTION_NAME" --region "$REGION"
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --environment "$env_vars" \
        --region "$REGION" >/dev/null
else
    log "Creating Lambda $LAMBDA_FUNCTION_NAME"
    aws lambda create-function \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --runtime python3.12 \
        --role "$ROLE_ARN" \
        --handler stream_replay.handler \
        --timeout 300 \
        --memory-size 512 \
        --zip-file "fileb://$SCRIPT_DIR/lambda.zip" \
        --environment "$env_vars" \
        --region "$REGION" >/dev/null
    aws lambda wait function-active --function-name "$LAMBDA_FUNCTION_NAME" --region "$REGION"
fi
rm -f "$SCRIPT_DIR/lambda.zip"

#
# 8. Event source mapping (with DLQ)
#
log "Ensuring event source mapping is in place"
EXISTING_ESM="$(aws lambda list-event-source-mappings \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --region "$REGION" \
    --query "EventSourceMappings[?EventSourceArn=='$SOURCE_STREAM_ARN'].UUID | [0]" \
    --output text)"
if [[ "$EXISTING_ESM" == "None" || -z "$EXISTING_ESM" ]]; then
    aws lambda create-event-source-mapping \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --event-source-arn "$SOURCE_STREAM_ARN" \
        --starting-position TRIM_HORIZON \
        --batch-size 100 \
        --maximum-batching-window-in-seconds 5 \
        --bisect-batch-on-function-error \
        --maximum-retry-attempts 3 \
        --function-response-types ReportBatchItemFailures \
        --destination-config "OnFailure={Destination=$DLQ_ARN}" \
        --region "$REGION" >/dev/null
    log "  created"
else
    log "  already exists ($EXISTING_ESM); updating destination config"
    aws lambda update-event-source-mapping \
        --uuid "$EXISTING_ESM" \
        --destination-config "OnFailure={Destination=$DLQ_ARN}" \
        --region "$REGION" >/dev/null
fi

#
# 9. CloudWatch alarms wired to SNS
#
log "Provisioning IteratorAge alarms (warn=$ITERATOR_AGE_WARN_MS ms, crit=$ITERATOR_AGE_CRIT_MS ms)"
for tier in WARN CRIT; do
    if [[ "$tier" == "WARN" ]]; then
        threshold="$ITERATOR_AGE_WARN_MS"
        name="ddb-migration-iterator-age-warning"
    else
        threshold="$ITERATOR_AGE_CRIT_MS"
        name="ddb-migration-iterator-age-critical"
    fi
    aws cloudwatch put-metric-alarm \
        --alarm-name "$name" \
        --alarm-description "Stream-replay Lambda IteratorAge (tier $tier)" \
        --namespace "AWS/Lambda" \
        --metric-name IteratorAge \
        --dimensions "Name=FunctionName,Value=$LAMBDA_FUNCTION_NAME" \
        --statistic Maximum \
        --period 60 \
        --evaluation-periods 5 \
        --threshold "$threshold" \
        --comparison-operator GreaterThanThreshold \
        --alarm-actions "$SNS_TOPIC_ARN" \
        --treat-missing-data notBreaching \
        --region "$REGION" >/dev/null
done
log "  done"

cat <<EOM

DEPLOY COMPLETE

  Source table:        $SOURCE_TABLE
  Target table:        $TARGET_TABLE  $( [[ "$CROSS_ACCOUNT" == "true" ]] && echo "(account $TARGET_ACCOUNT)" )
  Stream replay:       $LAMBDA_FUNCTION_NAME
  DLQ URL:             $DLQ_URL
  SNS topic:           $SNS_TOPIC_ARN
  Export bucket:       s3://$EXPORT_BUCKET

Next steps:
  1. Subscribe an email/Slack endpoint to the SNS topic for alarm notifications:
       aws sns subscribe --topic-arn $SNS_TOPIC_ARN --protocol email --notification-endpoint you@example.com --region $REGION
  2. Trigger the export:
       aws dynamodb export-table-to-point-in-time \\
         --table-arn \$(aws dynamodb describe-table --table-name $SOURCE_TABLE --region $REGION --query 'Table.TableArn' --output text) \\
         --s3-bucket $EXPORT_BUCKET --s3-prefix exports/ --export-format DYNAMODB_JSON --region $REGION
  3. Wait for the export to complete, then run the backfill:
       EXPORT_BUCKET=$EXPORT_BUCKET TARGET_TABLE=$TARGET_TABLE PARTITION_KEY=$PARTITION_KEY \\
         REGION=$REGION python scripts/backfill.py
  4. When ready, run the convergence gate:
       SOURCE_TABLE=$SOURCE_TABLE TARGET_TABLE=$TARGET_TABLE DLQ_URL=$DLQ_URL \\
         REGION=$REGION python scripts/convergence_check.py
EOM
