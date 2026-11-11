#!/usr/bin/env bash
# One-click demo of the zero-downtime migration toolkit.
#
# Provisions a tiny source/target pair, seeds 10K items, drives live writes
# during the migration, runs the convergence gate, then verifies the cutover.
# End-to-end runtime: ~12-15 minutes. Estimated cost: <$1 in a US region.
#
# Requires DDB_MIGRATION_DEMO_CONFIRM=yes — this provisions REAL AWS resources.
# Run ../teardown.sh CONFIRM=yes when done.

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOL_DIR="$(cd "$DEMO_DIR/.." && pwd)"

if [[ -f "$DEMO_DIR/config.env" ]]; then
    # shellcheck source=/dev/null
    source "$DEMO_DIR/config.env"
elif [[ -f "$TOOL_DIR/config.env" ]]; then
    # shellcheck source=/dev/null
    source "$TOOL_DIR/config.env"
else
    echo "ERROR: copy demo/config.example.env to demo/config.env first" >&2
    exit 2
fi

if [[ "${DDB_MIGRATION_DEMO_CONFIRM:-}" != "yes" ]]; then
    cat >&2 <<EOM
Refusing to run without explicit confirmation.

This will provision real AWS resources in account $(aws sts get-caller-identity --query Account --output text) / region ${REGION:-us-east-1}:
  - 2 DynamoDB tables (source + target)
  - 1 Lambda function + IAM role
  - 1 SQS queue (DLQ)
  - 1 SNS topic
  - 1 S3 bucket (export)
  - 2 CloudWatch alarms

Estimated cost: <\$1 for a 15-minute demo. Run ./teardown.sh CONFIRM=yes when done.

Re-run with DDB_MIGRATION_DEMO_CONFIRM=yes to proceed.
EOM
    exit 1
fi

REGION="${REGION:-us-east-1}"
: "${SOURCE_TABLE:?SOURCE_TABLE is required (see config.example.env)}"
: "${TARGET_TABLE:?TARGET_TABLE is required (see config.example.env)}"

step() { printf "\n========== %s ==========\n" "$*"; }

step "1/9  Creating source table $SOURCE_TABLE"
attr_defs="AttributeName=$PARTITION_KEY,AttributeType=S"
key_schema="AttributeName=$PARTITION_KEY,KeyType=HASH"
if [[ -n "${SORT_KEY:-}" ]]; then
    attr_defs="$attr_defs AttributeName=$SORT_KEY,AttributeType=S"
    key_schema="$key_schema AttributeName=$SORT_KEY,KeyType=RANGE"
fi
if ! aws dynamodb describe-table --table-name "$SOURCE_TABLE" --region "$REGION" >/dev/null 2>&1; then
    # shellcheck disable=SC2086
    aws dynamodb create-table \
        --table-name "$SOURCE_TABLE" \
        --attribute-definitions $attr_defs \
        --key-schema $key_schema \
        --billing-mode PAY_PER_REQUEST \
        --region "$REGION" >/dev/null
    aws dynamodb wait table-exists --table-name "$SOURCE_TABLE" --region "$REGION"
fi
aws dynamodb update-continuous-backups \
    --table-name "$SOURCE_TABLE" \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
    --region "$REGION" >/dev/null 2>&1 || true

step "2/9  Provisioning migration infrastructure (deploy.sh)"
( cd "$TOOL_DIR" && bash deploy.sh )

step "3/9  Seeding ${DEMO_ITEM_COUNT:-10000} items into $SOURCE_TABLE"
python "$DEMO_DIR/seed_data.py"

step "4/9  Triggering S3 export"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
EXPORT_BUCKET="${EXPORT_BUCKET:-ddb-migration-${ACCOUNT}-${REGION}}"
SOURCE_TABLE_ARN="$(aws dynamodb describe-table --table-name "$SOURCE_TABLE" --region "$REGION" --query 'Table.TableArn' --output text)"
EXPORT_ARN="$(aws dynamodb export-table-to-point-in-time \
    --table-arn "$SOURCE_TABLE_ARN" \
    --s3-bucket "$EXPORT_BUCKET" \
    --s3-prefix exports/ \
    --export-format DYNAMODB_JSON \
    --region "$REGION" \
    --query 'ExportDescription.ExportArn' --output text)"
echo "  export ARN: $EXPORT_ARN"

step "5/9  Starting live-writer in background (rate=${DEMO_LIVE_WRITE_RATE:-5}/s, duration=${DEMO_LIVE_WRITE_DURATION_SECS:-120}s)"
python "$DEMO_DIR/live_writer.py" &
LIVE_WRITER_PID=$!
trap 'kill $LIVE_WRITER_PID 2>/dev/null || true' EXIT

step "6/9  Waiting for export to complete"
while true; do
    status="$(aws dynamodb describe-export --export-arn "$EXPORT_ARN" --region "$REGION" --query 'ExportDescription.ExportStatus' --output text)"
    echo "  export status: $status"
    [[ "$status" == "COMPLETED" ]] && break
    [[ "$status" == "FAILED" ]] && { echo "  export failed"; exit 1; }
    sleep 30
done

step "7/9  Running backfill"
EXPORT_BUCKET="$EXPORT_BUCKET" python "$TOOL_DIR/scripts/backfill.py"

step "8/9  Waiting for live-writer to finish, then running convergence check"
wait "$LIVE_WRITER_PID" || true
trap - EXIT
DLQ_URL="$(aws sqs get-queue-url --queue-name ddb-migration-dlq --region "$REGION" --query 'QueueUrl' --output text)"
# 240s wait with 120s idle grace covers the case where the demo's traffic has stopped
# and the Lambda is no longer emitting IteratorAge datapoints.
DLQ_URL="$DLQ_URL" python "$TOOL_DIR/scripts/convergence_check.py" --max-wait-seconds 240

step "9/9  Verifying sample of items"
python "$TOOL_DIR/scripts/verify_cutover.py" --sample-size 500

cat <<EOM

DEMO PASSED

  Source: $SOURCE_TABLE
  Target: $TARGET_TABLE

To clean up:
  cd $TOOL_DIR
  CONFIRM=yes ./teardown.sh
  aws dynamodb delete-table --table-name $SOURCE_TABLE --region $REGION
  aws dynamodb delete-table --table-name $TARGET_TABLE --region $REGION
EOM
