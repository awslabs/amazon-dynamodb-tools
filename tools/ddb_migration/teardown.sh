#!/usr/bin/env bash
# Removes everything deploy.sh provisioned. Best-effort: prints anything it
# could not remove. Refuses to run unless CONFIRM=yes.
#
# Does NOT delete:
#   - the source table
#   - the target table (you must verify cutover and clean it up yourself)
#   - data in the S3 export bucket (deletes the bucket only after empty)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/config.env" ]]; then
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/config.env"
fi

if [[ "${CONFIRM:-}" != "yes" ]]; then
    cat >&2 <<EOM
Refusing to run without explicit confirmation.

This will delete:
  - the stream-replay Lambda + its event source mapping
  - the IAM role
  - the SQS DLQ
  - the SNS alerts topic
  - the IteratorAge CloudWatch alarms
  - the S3 export bucket (only if empty)

Re-run with CONFIRM=yes to proceed.
EOM
    exit 1
fi

REGION="${REGION:-us-east-1}"
LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-ddb-migration-stream-replay}"
LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-ddb-migration-stream-replay-role}"
DLQ_NAME="${DLQ_NAME:-ddb-migration-dlq}"
SNS_TOPIC_NAME="${SNS_TOPIC_NAME:-ddb-migration-alerts}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
EXPORT_BUCKET="${EXPORT_BUCKET:-ddb-migration-${ACCOUNT}-${REGION}}"

step() { printf "\n[teardown] %s\n" "$*"; }
errs=0
try() { "$@" 2>/dev/null || { printf "  (skipped/failed: %s)\n" "$*" >&2; errs=$((errs+1)); }; }

step "Deleting event source mappings for $LAMBDA_FUNCTION_NAME"
mapfile -t esms < <(aws lambda list-event-source-mappings \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --region "$REGION" \
    --query 'EventSourceMappings[].UUID' --output text 2>/dev/null | tr '\t' '\n')
for esm in "${esms[@]}"; do
    [[ -z "$esm" ]] && continue
    try aws lambda delete-event-source-mapping --uuid "$esm" --region "$REGION"
done

step "Deleting Lambda $LAMBDA_FUNCTION_NAME"
try aws lambda delete-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$REGION"

step "Deleting IAM role $LAMBDA_ROLE_NAME"
try aws iam delete-role-policy --role-name "$LAMBDA_ROLE_NAME" --policy-name "ddb-migration-stream-replay-inline"
try aws iam delete-role --role-name "$LAMBDA_ROLE_NAME"

step "Deleting CloudWatch alarms"
try aws cloudwatch delete-alarms \
    --alarm-names ddb-migration-iterator-age-warning ddb-migration-iterator-age-critical \
    --region "$REGION"

step "Deleting SNS topic $SNS_TOPIC_NAME"
sns_arn="$(aws sns list-topics --region "$REGION" --query "Topics[?ends_with(TopicArn, ':$SNS_TOPIC_NAME')].TopicArn | [0]" --output text 2>/dev/null)"
if [[ "$sns_arn" != "None" && -n "$sns_arn" ]]; then
    try aws sns delete-topic --topic-arn "$sns_arn" --region "$REGION"
fi

step "Deleting SQS DLQ $DLQ_NAME"
dlq_url="$(aws sqs get-queue-url --queue-name "$DLQ_NAME" --region "$REGION" --query 'QueueUrl' --output text 2>/dev/null || echo "")"
if [[ -n "$dlq_url" && "$dlq_url" != "None" ]]; then
    try aws sqs delete-queue --queue-url "$dlq_url" --region "$REGION"
fi

step "Deleting export bucket s3://$EXPORT_BUCKET (must be empty)"
try aws s3api delete-bucket --bucket "$EXPORT_BUCKET" --region "$REGION"

if [[ $errs -gt 0 ]]; then
    printf "\n[teardown] completed with %d step(s) skipped or failed; see warnings above\n" "$errs"
    exit 1
fi
printf "\n[teardown] complete\n"
