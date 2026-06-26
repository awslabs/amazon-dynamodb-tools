#!/usr/bin/env bash
set -euo pipefail

# Switch e2e CI from one AWS account to another.
# Tears down the old account, bootstraps the new one.
#
# Prerequisites:
#   - AWS CLI authenticated to the OLD account (for teardown)
#   - You'll be prompted to switch credentials before bootstrap
#
# Usage:
#   .github/scripts/e2e-switch-account.sh \
#     --old-account-id 654654401288 \
#     --new-account-id 111222333444 \
#     --region us-east-1 \
#     --read-table tiny-boat \
#     --write-table mini-boat \
#     --repos "awslabs/amazon-dynamodb-tools,relentlesscol/amazon-dynamodb-tools"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  echo "Usage: $0 --old-account-id ID --new-account-id ID --region REGION --read-table TABLE --write-table TABLE --repos REPO1,REPO2"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --old-account-id) OLD_ACCOUNT="$2"; shift 2 ;;
    --new-account-id) NEW_ACCOUNT="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --read-table) READ_TABLE="$2"; shift 2 ;;
    --write-table) WRITE_TABLE="$2"; shift 2 ;;
    --repos) REPOS="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -z "${OLD_ACCOUNT:-}" || -z "${NEW_ACCOUNT:-}" || -z "${REGION:-}" || -z "${READ_TABLE:-}" || -z "${WRITE_TABLE:-}" || -z "${REPOS:-}" ]] && usage

echo "╔══════════════════════════════════════════════════╗"
echo "║  E2E Account Switch: ${OLD_ACCOUNT} → ${NEW_ACCOUNT}  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# --- Phase 1: Teardown old account ---
echo "── Phase 1: Teardown (account ${OLD_ACCOUNT}) ──"
echo ""
echo "Ensure AWS CLI is authenticated to ${OLD_ACCOUNT}."
read -rp "Press Enter to continue (or Ctrl+C to abort)..."
echo ""

"${SCRIPT_DIR}/e2e-teardown.sh" --account-id "${OLD_ACCOUNT}" --repos "${REPOS}"

echo ""

# --- Phase 2: Bootstrap new account ---
echo "── Phase 2: Bootstrap (account ${NEW_ACCOUNT}) ──"
echo ""
echo "Switch AWS CLI credentials to ${NEW_ACCOUNT} now."
echo "  e.g.: ada credentials update --account ${NEW_ACCOUNT} --provider isengard --role Admin --once"
echo ""
read -rp "Press Enter when ready (or Ctrl+C to abort)..."
echo ""

"${SCRIPT_DIR}/e2e-bootstrap.sh" \
  --account-id "${NEW_ACCOUNT}" \
  --region "${REGION}" \
  --read-table "${READ_TABLE}" \
  --write-table "${WRITE_TABLE}" \
  --repos "${REPOS}"

echo ""
echo "==> Account switch complete: ${OLD_ACCOUNT} → ${NEW_ACCOUNT}"
