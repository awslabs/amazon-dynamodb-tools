#!/usr/bin/env bash
set -euo pipefail

# Bootstrap GitHub Actions e2e infrastructure in a given AWS account.
# Creates: OIDC identity provider, IAM role, inline policy, GitHub secrets.
#
# Prerequisites:
#   - AWS CLI authenticated to the target account (Admin role)
#   - gh CLI authenticated with repo admin access
#   - jq installed
#
# Usage:
#   .github/scripts/e2e-bootstrap.sh \
#     --account-id 123456789012 \
#     --region us-east-1 \
#     --read-table tiny-boat \
#     --write-table mini-boat \
#     --repos "awslabs/amazon-dynamodb-tools,relentlesscol/amazon-dynamodb-tools"

ROLE_NAME="github-actions-e2e-runner"
POLICY_NAME="e2e-test-access"

usage() {
  echo "Usage: $0 --account-id ID --region REGION --read-table TABLE --write-table TABLE --repos REPO1,REPO2"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --account-id) ACCOUNT_ID="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --read-table) READ_TABLE="$2"; shift 2 ;;
    --write-table) WRITE_TABLE="$2"; shift 2 ;;
    --repos) IFS=',' read -ra REPOS <<< "$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -z "${ACCOUNT_ID:-}" || -z "${REGION:-}" || -z "${READ_TABLE:-}" || -z "${WRITE_TABLE:-}" || ${#REPOS[@]} -eq 0 ]] && usage

echo "==> Bootstrapping e2e CI in account ${ACCOUNT_ID} (${REGION})"

# --- OIDC Provider ---
OIDC_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
if aws iam get-open-id-connect-provider --open-id-connect-provider-arn "${OIDC_ARN}" >/dev/null 2>&1; then
  echo "    OIDC provider already exists, skipping"
else
  echo "    Creating OIDC provider..."
  aws iam create-open-id-connect-provider \
    --url "https://token.actions.githubusercontent.com" \
    --client-id-list "sts.amazonaws.com" \
    --thumbprint-list "6938fd4d98bab03faadb97b34396831e3780aea1" "1c58a3a8518e8759bf075b76b750d4f2df264fcd" \
    --output text --query 'OpenIDConnectProviderArn'
fi

# --- Trust Policy ---
SUB_CONDITIONS=$(printf '"%s"' "repo:${REPOS[0]}:ref:refs/heads/main")
for repo in "${REPOS[@]:1}"; do
  SUB_CONDITIONS+=", \"repo:${repo}:ref:refs/heads/main\""
done

TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "${OIDC_ARN}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": [${SUB_CONDITIONS}]
        }
      }
    }
  ]
}
EOF
)

# --- IAM Role ---
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "    Role ${ROLE_NAME} exists, updating trust policy..."
  aws iam update-assume-role-policy --role-name "${ROLE_NAME}" --policy-document "${TRUST_POLICY}"
else
  echo "    Creating role ${ROLE_NAME}..."
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --description "GitHub Actions role for bulk_executor e2e tests" \
    --output text --query 'Role.Arn'
fi

# --- Inline Policy ---
echo "    Attaching inline policy ${POLICY_NAME}..."
PERMISSIONS_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DynamoDBNamedTables",
      "Effect": "Allow",
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:DeleteItem",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:UpdateItem",
        "dynamodb:DescribeExport",
        "dynamodb:ExportTableToPointInTime",
        "dynamodb:DescribeContinuousBackups",
        "dynamodb:UpdateContinuousBackups"
      ],
      "Resource": [
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${READ_TABLE}",
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${READ_TABLE}/*",
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${WRITE_TABLE}",
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${WRITE_TABLE}/*"
      ]
    },
    {
      "Sid": "DynamoDBTransientTables",
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DeleteTable",
        "dynamodb:DescribeTable",
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:UpdateItem",
        "dynamodb:UpdateContinuousBackups",
        "dynamodb:DescribeContinuousBackups",
        "dynamodb:TagResource"
      ],
      "Resource": [
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/bulk-e2e-*"
      ]
    },
    {
      "Sid": "GlueJobAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetJob",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:StartJobRun",
        "glue:BatchStopJobRun"
      ],
      "Resource": [
        "arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/bulk_dynamodb"
      ]
    },
    {
      "Sid": "S3GlueBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::aws-glue-bulk-dynamodb-*",
        "arn:aws:s3:::aws-glue-bulk-dynamodb-*/*"
      ]
    },
    {
      "Sid": "PassGlueRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/AWSGlueServiceRole*",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "glue.amazonaws.com"
        }
      }
    },
    {
      "Sid": "CloudWatchLogsRead",
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents",
        "logs:StartLiveTail"
      ],
      "Resource": "*"
    },
    {
      "Sid": "STSIdentity",
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "${POLICY_NAME}" \
  --policy-document "${PERMISSIONS_POLICY}"

# --- GitHub Secrets ---
echo "    Setting GitHub secrets..."
for repo in "${REPOS[@]}"; do
  echo "      ${repo}"
  gh secret set E2E_AWS_ROLE_ARN   --repo "${repo}" --body "${ROLE_ARN}"
  gh secret set E2E_AWS_ACCOUNT_ID --repo "${repo}" --body "${ACCOUNT_ID}"
  gh secret set E2E_AWS_REGION     --repo "${repo}" --body "${REGION}"
  gh secret set E2E_READ_TABLE     --repo "${repo}" --body "${READ_TABLE}"
  gh secret set E2E_WRITE_TABLE    --repo "${repo}" --body "${WRITE_TABLE}"
done

echo ""
echo "==> Bootstrap complete"
echo "    OIDC:    ${OIDC_ARN}"
echo "    Role:    ${ROLE_ARN}"
echo "    Repos:   ${REPOS[*]}"
echo "    Region:  ${REGION}"
echo "    Tables:  ${READ_TABLE} (read), ${WRITE_TABLE} (write)"
