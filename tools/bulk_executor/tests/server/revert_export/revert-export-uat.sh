#!/bin/bash
# End-to-end UAT for revert-export:
#   1. Create a table with PITR enabled and seed 5 users
#   2. Full export + load into a restored-* table (baseline for diff)
#   3. Make mutations (3 adds, 1 edit, 1 delete)
#   4. Verify mutations caused differences vs baseline
#   5. Incremental export capturing only the mutations
#   6. Revert the original table using the incremental export
#   7. Diff the original table against the restored baseline to verify revert worked
#   8. Optionally delete both DynamoDB tables (S3 exports are retained)
#
# Usage: ./revert-export-uat.sh [table-name] [s3-path]
#   s3-path: s3://bucket or s3://bucket/prefix (e.g. s3://my-bucket/revert-export-uat)

set -euo pipefail
export AWS_PAGER=""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BULK="$PROJECT_ROOT/bulk"
DIFF_SUCCESS_MSG="No differences found"

wait_for_export() {
  local export_arn="$1"
  local start=$SECONDS
  printf "  Waiting for export to complete... (0s)"
  while true; do
    STATUS=$(aws dynamodb describe-export --export-arn "$export_arn" --query "ExportDescription.ExportStatus" --output text)
    if [ "$STATUS" = "COMPLETED" ]; then
      printf "\r  Export completed. (%ds)                                      \n" $((SECONDS - start))
      return 0
    elif [ "$STATUS" = "FAILED" ]; then
      printf "\r  Error: Export failed. (%ds)                                  \n" $((SECONDS - start))
      exit 1
    fi
    sleep 30
    printf "\r  Waiting for export to complete... (%ds)" $((SECONDS - start))
  done
}

TABLE_NAME="${1:-revert-export-uat}"
S3_PATH="${2:-}"

if [ -z "$S3_PATH" ]; then
  read -p "Enter S3 path for exports (e.g. s3://my-bucket or s3://my-bucket/prefix): " S3_PATH
fi

if [ -z "$S3_PATH" ]; then
  echo "Error: S3 path is required."
  exit 1
fi

# Extract bucket and prefix from s3://bucket/prefix/ or s3://bucket
S3_PATH="${S3_PATH#s3://}"
S3_PATH="${S3_PATH%/}"
S3_BUCKET="${S3_PATH%%/*}"
S3_PREFIX="${S3_PATH#*/}"
if [ "$S3_PREFIX" = "$S3_BUCKET" ]; then
  S3_PREFIX=""
fi

# --- Step 1: Create table, enable PITR, seed data ---
echo "=== Step 1: Creating table '$TABLE_NAME' ==="

if aws dynamodb describe-table --table-name "$TABLE_NAME" &>/dev/null; then
  echo "Error: Table '$TABLE_NAME' already exists. Delete it first or use a different name."
  exit 1
fi

echo "  Creating table..."
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions AttributeName=user_id,AttributeType=S \
  --key-schema AttributeName=user_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > /dev/null

echo "  Waiting for table to be active..."
aws dynamodb wait table-exists --table-name "$TABLE_NAME"

echo "  Enabling PITR..."
aws dynamodb update-continuous-backups \
  --table-name "$TABLE_NAME" \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true > /dev/null

echo "  Writing 5 seed users..."
aws dynamodb batch-write-item --request-items "{
  \"$TABLE_NAME\": [
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-001\"},\"name\":{\"S\":\"Foo Bar\"},\"email\":{\"S\":\"foo@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-002\"},\"name\":{\"S\":\"Baz Qux\"},\"email\":{\"S\":\"baz@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-003\"},\"name\":{\"S\":\"Waldo Corge\"},\"email\":{\"S\":\"waldo@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-004\"},\"name\":{\"S\":\"Grault Garply\"},\"email\":{\"S\":\"grault@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-005\"},\"name\":{\"S\":\"Plugh Xyzzy\"},\"email\":{\"S\":\"plugh@example.com\"}}}}
  ]
}" > /dev/null
echo "  Done."

# --- Step 2: Full export + load into restored table ---
echo ""
echo "=== Step 2: Full export + restore baseline ==="

TABLE_ARN=$(aws dynamodb describe-table --table-name "$TABLE_NAME" --query "Table.TableArn" --output text)
RESTORED_TABLE_NAME="restored-${TABLE_NAME}"

echo "  Starting full export..."
EXPORT_ARGS="--table-arn $TABLE_ARN --s3-bucket $S3_BUCKET --export-type FULL_EXPORT"
if [ -n "$S3_PREFIX" ]; then
  EXPORT_ARGS="$EXPORT_ARGS --s3-prefix $S3_PREFIX"
fi
EXPORT_OUTPUT=$(aws dynamodb export-table-to-point-in-time $EXPORT_ARGS)
EXPORT_ARN=$(echo "$EXPORT_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['ExportDescription']['ExportArn'])")
echo "  Export ARN: $EXPORT_ARN"

wait_for_export "$EXPORT_ARN"

echo "  Creating restored table '$RESTORED_TABLE_NAME'..."
aws dynamodb create-table \
  --table-name "$RESTORED_TABLE_NAME" \
  --attribute-definitions AttributeName=user_id,AttributeType=S \
  --key-schema AttributeName=user_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > /dev/null
aws dynamodb wait table-exists --table-name "$RESTORED_TABLE_NAME"

EXPORT_S3_PREFIX=$(aws dynamodb describe-export --export-arn "$EXPORT_ARN" \
  --query "ExportDescription.S3Prefix" --output text)
EXPORT_ID=$(echo "$EXPORT_ARN" | grep -o '[^/]*$')
if [ "$EXPORT_S3_PREFIX" = "None" ] || [ -z "$EXPORT_S3_PREFIX" ]; then
  FULL_EXPORT_S3_PATH="s3://${S3_BUCKET}/AWSDynamoDB/${EXPORT_ID}"
else
  FULL_EXPORT_S3_PATH="s3://${S3_BUCKET}/${EXPORT_S3_PREFIX}/AWSDynamoDB/${EXPORT_ID}"
fi

echo "  Enabling PITR on restored table..."
aws dynamodb update-continuous-backups \
  --table-name "$RESTORED_TABLE_NAME" \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true > /dev/null

echo "  Loading full export into '$RESTORED_TABLE_NAME'..."
(cd "$PROJECT_ROOT" && ./bulk load-export --table "$RESTORED_TABLE_NAME" --s3-path "$FULL_EXPORT_S3_PATH") > /dev/null 2>&1
echo "  Baseline ready."

# --- Step 3: Make mutations ---
echo ""
echo "=== Step 3: Making mutations ==="

EXPORT_START_TIME=$(date -u +%s)
echo "  Export start time: $EXPORT_START_TIME"

echo "  Adding 3 new users..."
aws dynamodb batch-write-item --request-items "{
  \"$TABLE_NAME\": [
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-006\"},\"name\":{\"S\":\"Quux Norf\"},\"email\":{\"S\":\"quux@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-007\"},\"name\":{\"S\":\"Thud Blat\"},\"email\":{\"S\":\"thud@example.com\"}}}},
    {\"PutRequest\":{\"Item\":{\"user_id\":{\"S\":\"user-008\"},\"name\":{\"S\":\"Zim Zam\"},\"email\":{\"S\":\"zim@example.com\"}}}}
  ]
}" > /dev/null

echo "  Editing user-001 (updating email)..."
aws dynamodb put-item --table-name "$TABLE_NAME" \
  --item '{"user_id":{"S":"user-001"},"name":{"S":"Foo Bar"},"email":{"S":"foo.updated@example.com"}}' > /dev/null

echo "  Deleting user-005..."
aws dynamodb delete-item --table-name "$TABLE_NAME" \
  --key '{"user_id":{"S":"user-005"}}' > /dev/null

echo "  Mutations complete."

# --- Step 4: Verify mutations caused differences ---
echo ""
echo "=== Step 4: Verifying mutations caused differences ==="
DIFF_OUTPUT=$( (cd "$PROJECT_ROOT" && ./bulk diff --table "$TABLE_NAME" --table2 "$RESTORED_TABLE_NAME") 2>&1 )
if echo "$DIFF_OUTPUT" | grep -q "$DIFF_SUCCESS_MSG"; then
  printf "  \033[31mFAIL\033[0m: Expected differences after mutations but found none.\n"
  exit 1
else
  printf "  \033[32mPASS\033[0m: Differences detected as expected.\n"
fi

RESUME_TIME=$(date -v+15M +"%H:%M:%S" 2>/dev/null || date -d "+15 minutes" +"%H:%M:%S")
echo ""
echo "  Waiting 15 minutes for incremental export window (resuming at $RESUME_TIME — go grab a coffee)..."
sleep 900

EXPORT_END_TIME=$(date -u +%s)
echo "  Export end time: $EXPORT_END_TIME"

# --- Step 5: Incremental export ---
echo ""
echo "=== Step 5: Incremental export ==="

echo "  Starting incremental export..."
INCR_EXPORT_ARGS="--table-arn $TABLE_ARN --s3-bucket $S3_BUCKET --export-type INCREMENTAL_EXPORT"
if [ -n "$S3_PREFIX" ]; then
  INCR_EXPORT_ARGS="$INCR_EXPORT_ARGS --s3-prefix $S3_PREFIX"
fi
INCR_EXPORT_OUTPUT=$(aws dynamodb export-table-to-point-in-time $INCR_EXPORT_ARGS \
  --incremental-export-specification "ExportFromTime=$EXPORT_START_TIME,ExportToTime=$EXPORT_END_TIME,ExportViewType=NEW_AND_OLD_IMAGES")
INCR_EXPORT_ARN=$(echo "$INCR_EXPORT_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['ExportDescription']['ExportArn'])")
echo "  Export ARN: $INCR_EXPORT_ARN"

wait_for_export "$INCR_EXPORT_ARN"

INCR_EXPORT_ID=$(echo "$INCR_EXPORT_ARN" | grep -o '[^/]*$')
INCR_S3_PREFIX=$(aws dynamodb describe-export --export-arn "$INCR_EXPORT_ARN" \
  --query "ExportDescription.S3Prefix" --output text)
if [ "$INCR_S3_PREFIX" = "None" ] || [ -z "$INCR_S3_PREFIX" ]; then
  INCR_EXPORT_S3_PATH="s3://${S3_BUCKET}/AWSDynamoDB/${INCR_EXPORT_ID}"
else
  INCR_EXPORT_S3_PATH="s3://${S3_BUCKET}/${INCR_S3_PREFIX}/AWSDynamoDB/${INCR_EXPORT_ID}"
fi

# --- Step 6: Revert ---
echo ""
echo "=== Step 6: Reverting mutations ==="
(cd "$PROJECT_ROOT" && ./bulk revert-export --table "$TABLE_NAME" --s3-path "$INCR_EXPORT_S3_PATH") > /dev/null 2>&1
echo "  Done."

# --- Step 7: Diff ---
echo ""
echo "=== Step 7: Verifying revert via diff ==="
DIFF_OUTPUT=$( (cd "$PROJECT_ROOT" && ./bulk diff --table "$TABLE_NAME" --table2 "$RESTORED_TABLE_NAME") 2>&1 )
if echo "$DIFF_OUTPUT" | grep -q "$DIFF_SUCCESS_MSG"; then
  printf "  \033[32mPASS\033[0m: No differences found. Revert-export worked correctly.\n"
else
  printf "  \033[31mFAIL\033[0m: Differences detected after revert:\n"
  echo "$DIFF_OUTPUT"
  exit 1
fi

# --- Step 8: Cleanup ---
echo ""
echo "=== Step 8: Cleanup ==="
echo "Note: S3 exports at s3://${S3_BUCKET} will NOT be deleted automatically."
read -p "Delete both DynamoDB tables ('$TABLE_NAME' and '$RESTORED_TABLE_NAME')? [y/N] " CLEANUP
if [ "$CLEANUP" = "y" ] || [ "$CLEANUP" = "Y" ]; then
  aws dynamodb delete-table --table-name "$TABLE_NAME" > /dev/null
  aws dynamodb delete-table --table-name "$RESTORED_TABLE_NAME" > /dev/null
  echo "Both tables deleted."
else
  echo "Tables retained. Delete manually when done."
fi