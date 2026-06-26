#!/usr/bin/env bash
set -euo pipefail

# Tear down GitHub Actions e2e infrastructure from a given AWS account.
# Removes: inline policy, IAM role, OIDC provider, GitHub secrets.
#
# Usage:
#   .github/scripts/e2e-teardown.sh \
#     --account-id 123456789012 \
#     --repos "awslabs/amazon-dynamodb-tools,relentlesscol/amazon-dynamodb-tools"

ROLE_NAME="github-actions-e2e-runner"
POLICY_NAME="e2e-test-access"

usage() {
  echo "Usage: $0 --account-id ID --repos REPO1,REPO2"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --account-id) ACCOUNT_ID="$2"; shift 2 ;;
    --repos) IFS=',' read -ra REPOS <<< "$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -z "${ACCOUNT_ID:-}" || ${#REPOS[@]} -eq 0 ]] && usage

echo "==> Tearing down e2e CI from account ${ACCOUNT_ID}"

# --- Inline Policy ---
if aws iam get-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}" >/dev/null 2>&1; then
  echo "    Deleting inline policy ${POLICY_NAME}..."
  aws iam delete-role-policy --role-name "${ROLE_NAME}" --policy-name "${POLICY_NAME}"
else
  echo "    No inline policy found, skipping"
fi

# --- IAM Role ---
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "    Deleting role ${ROLE_NAME}..."
  aws iam delete-role --role-name "${ROLE_NAME}"
else
  echo "    No role found, skipping"
fi

# --- OIDC Provider ---
OIDC_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
if aws iam get-open-id-connect-provider --open-id-connect-provider-arn "${OIDC_ARN}" >/dev/null 2>&1; then
  echo "    Deleting OIDC provider..."
  aws iam delete-open-id-connect-provider --open-id-connect-provider-arn "${OIDC_ARN}"
else
  echo "    No OIDC provider found, skipping"
fi

# --- GitHub Secrets ---
echo "    Removing GitHub secrets..."
for repo in "${REPOS[@]}"; do
  echo "      ${repo}"
  gh secret delete E2E_AWS_ROLE_ARN   --repo "${repo}" 2>/dev/null || true
  gh secret delete E2E_AWS_ACCOUNT_ID --repo "${repo}" 2>/dev/null || true
  gh secret delete E2E_AWS_REGION     --repo "${repo}" 2>/dev/null || true
  gh secret delete E2E_READ_TABLE     --repo "${repo}" 2>/dev/null || true
  gh secret delete E2E_WRITE_TABLE    --repo "${repo}" 2>/dev/null || true
done

echo ""
echo "==> Teardown complete for account ${ACCOUNT_ID}"
