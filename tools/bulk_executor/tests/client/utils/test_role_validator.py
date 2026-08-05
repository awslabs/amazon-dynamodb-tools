"""Unit tests for utils.role_validator — custom role permission warnings.

When users pass --XRole with a custom IAM role name, validate_custom_role_permissions
checks it against the README's documented minimums for a custom Glue role:

  1. name starts with AWSGlueServiceRole
  2. trust policy allows glue.amazonaws.com
  3. AWSGlueServiceRole managed policy attached
  4. pricing:GetProducts (managed OR inline)
  5. servicequotas:GetServiceQuota + GetAWSDefaultServiceQuota (managed OR inline)
  6. application-autoscaling:DescribeScalableTargets (soft — job still runs without it)
  7. some DynamoDB access (managed OR a restrictive per-table policy)

The function WARNS (returned as strings, logged by the caller) but never exits —
it is advisory, not blocking. Checks 4-7 are evaluated against the actions the
role EFFECTIVELY allows (attached managed-policy documents + inline policies),
not by matching policy ARNs — so documented lockdown setups (inline pricing,
restrictive DynamoDB) do NOT false-warn. That effective-permission contract is
what these tests pin down; the old ARN-matching contract is intentionally gone.
"""

from unittest.mock import MagicMock, patch

import pytest

from utils.role_validator import validate_custom_role_permissions

GLUE_BASELINE_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
DDB_READONLY_ARN = "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"
DDB_FULL_ARN = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
PRICING_FULL_ARN = "arn:aws:iam::aws:policy/AWSPriceListServiceFullAccess"
QUOTAS_RO_ARN = "arn:aws:iam::aws:policy/ServiceQuotasReadOnlyAccess"

GOOD_ROLE_NAME = "AWSGlueServiceRole-MyCustom"


def _trust(*principals):
    services = list(principals) if len(principals) > 1 else principals[0]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": services},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def _managed_doc(*actions):
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": list(actions), "Resource": "*"}],
    }


# Canonical action sets for the common AWS-managed policies, keyed by ARN. A
# role built with the AWS-managed policies exposes exactly these capabilities.
_MANAGED_POLICY_ACTIONS = {
    GLUE_BASELINE_ARN: ["glue:*", "s3:GetObject", "logs:CreateLogGroup"],
    DDB_READONLY_ARN: ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
    DDB_FULL_ARN: ["dynamodb:*"],
    PRICING_FULL_ARN: ["pricing:*"],
    QUOTAS_RO_ARN: [
        "servicequotas:GetServiceQuota",
        "servicequotas:GetAWSDefaultServiceQuota",
        "servicequotas:ListServiceQuotas",
    ],
}


class _FakeIam:
    """Minimal IAM stub driven by declarative role state.

    Models get_role, list_attached_role_policies, get_policy(_version),
    list_role_policies and get_role_policy well enough to exercise the
    validator's real read path — instead of hand-feeding one canned response.
    """

    def __init__(self, *, trust, attached_arns=(), inline=None,
                 fail_on=None, managed_actions=None):
        self._trust = trust
        self._attached = list(attached_arns)
        self._inline = dict(inline or {})          # name -> policy document
        self._fail_on = set(fail_on or ())          # method names that raise
        # ARN -> [actions]; defaults cover the common AWS-managed policies.
        self._managed = dict(_MANAGED_POLICY_ACTIONS)
        if managed_actions:
            self._managed.update(managed_actions)

    def _maybe_fail(self, name):
        if name in self._fail_on:
            raise Exception(f"simulated IAM failure: {name}")

    def get_role(self, RoleName):
        self._maybe_fail("get_role")
        return {"Role": {"AssumeRolePolicyDocument": self._trust}}

    def list_attached_role_policies(self, RoleName):
        self._maybe_fail("list_attached_role_policies")
        return {"AttachedPolicies": [{"PolicyArn": a} for a in self._attached]}

    def get_policy(self, PolicyArn):
        self._maybe_fail("get_policy")
        return {"Policy": {"DefaultVersionId": "v1"}}

    def get_policy_version(self, PolicyArn, VersionId):
        self._maybe_fail("get_policy_version")
        actions = self._managed.get(PolicyArn, ["*"])
        return {"PolicyVersion": {"Document": _managed_doc(*actions)}}

    def list_role_policies(self, RoleName):
        self._maybe_fail("list_role_policies")
        return {"PolicyNames": list(self._inline.keys())}

    def get_role_policy(self, RoleName, PolicyName):
        self._maybe_fail("get_role_policy")
        return {"PolicyDocument": self._inline[PolicyName]}


def _fully_valid_iam():
    """A role that satisfies every documented requirement via managed policies
    plus one inline policy for pricing/quota/autoscaling (what bootstrap builds)."""
    inline = {
        "MinimalExtras": _managed_doc(
            "pricing:GetProducts",
            "servicequotas:GetServiceQuota",
            "servicequotas:GetAWSDefaultServiceQuota",
            "application-autoscaling:DescribeScalableTargets",
        )
    }
    return _FakeIam(
        trust=_trust("glue.amazonaws.com"),
        attached_arns=[GLUE_BASELINE_ARN, DDB_READONLY_ARN],
        inline=inline,
    )


class TestFullyValidRole:
    def test_no_warnings_when_all_requirements_met(self):
        warnings = validate_custom_role_permissions(_fully_valid_iam(), GOOD_ROLE_NAME)
        assert warnings == []


class TestNamePrefix:
    def test_warns_when_name_lacks_glue_prefix(self):
        iam = _fully_valid_iam()
        warnings = validate_custom_role_permissions(iam, "MyCustomRole")
        assert any("AWSGlueServiceRole" in w and "does not start" in w for w in warnings)

    def test_no_prefix_warning_when_name_has_prefix(self):
        iam = _fully_valid_iam()
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("does not start" in w for w in warnings)


class TestTrustPolicy:
    def test_warns_when_trust_missing_glue_principal(self):
        iam = _FakeIam(
            trust=_trust("ec2.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts", "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("glue.amazonaws.com" in w for w in warnings)

    def test_no_trust_warning_when_glue_in_principal_list(self):
        iam = _FakeIam(
            trust=_trust("lambda.amazonaws.com", "glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts", "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("glue.amazonaws.com" in w for w in warnings)


class TestGlueBaselinePolicy:
    def test_warns_when_baseline_managed_policy_missing(self):
        iam = _fully_valid_iam()
        iam._attached = [DDB_READONLY_ARN]  # drop the baseline policy
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("AWSGlueServiceRole managed policy" in w for w in warnings)


class TestPricingCapability:
    def test_warns_when_no_pricing_access(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("pricing:GetProducts" in w for w in warnings)

    def test_pricing_via_managed_policy_satisfies(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN, PRICING_FULL_ARN],
            inline={"x": _managed_doc(
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in w for w in warnings)

    def test_pricing_via_inline_lockdown_satisfies(self):
        # The documented "maximum lockdown" path: inline pricing:GetProducts.
        iam = _fully_valid_iam()
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in w for w in warnings)


class TestQuotaCapability:
    def test_warns_when_no_quota_access(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("servicequotas" in w.lower() for w in warnings)

    def test_quota_via_managed_policy_satisfies(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN, QUOTAS_RO_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("Service Quotas" in w for w in warnings)

    def test_partial_quota_still_warns(self):
        # Only one of the two required servicequotas actions present.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:GetServiceQuota",  # missing GetAWSDefaultServiceQuota
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("servicequotas" in w.lower() for w in warnings)


class TestAutoscalingCapability:
    def test_warns_when_no_autoscaling_but_marks_soft(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        autoscaling = [w for w in warnings if "application-autoscaling" in w]
        assert autoscaling, "expected an autoscaling warning"
        assert any("still runs" in w for w in autoscaling), (
            "autoscaling warning should note the job still runs (soft requirement)"
        )


class TestDynamoDbCapability:
    def test_warns_when_no_dynamodb_access(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("DynamoDB" in w for w in warnings)

    def test_readonly_managed_dynamodb_satisfies(self):
        iam = _fully_valid_iam()  # uses DDB_READONLY_ARN
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("DynamoDB" in w for w in warnings)

    def test_restrictive_inline_dynamodb_satisfies(self):
        # README blesses "a more restrictive policy targeting specific tables".
        # ARN-matching would false-warn here; effective-action checks must not.
        inline = {
            "MinimalExtras": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            ),
            "ScopedDynamo": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["dynamodb:GetItem", "dynamodb:Query"],
                    "Resource": "arn:aws:dynamodb:us-east-1:123456789012:table/only-this-one",
                }],
            },
        }
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN],  # NO managed DynamoDB policy
            inline=inline,
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("DynamoDB" in w for w in warnings)


class TestPolicyDocumentShapes:
    """The action collector must handle the shapes real IAM documents take:
    a single statement (dict, not list), a single action (str, not list),
    Deny statements (ignored), and a full '*' wildcard (grants everything)."""

    def test_admin_star_wildcard_satisfies_every_capability(self):
        # A single inline statement granting "*" should satisfy pricing, quota,
        # autoscaling and DynamoDB all at once.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN],
            inline={"Admin": {
                "Version": "2012-10-17",
                "Statement": {  # single statement as a dict, not a list
                    "Effect": "Allow", "Action": "*", "Resource": "*",
                },
            }},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert warnings == [], f"'*' should satisfy everything; got {warnings!r}"

    def test_single_string_action_is_collected(self):
        # DynamoDB granted via a single string action (not a list).
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN],
            inline={
                "Extras": _managed_doc(
                    "pricing:GetProducts",
                    "servicequotas:GetServiceQuota",
                    "servicequotas:GetAWSDefaultServiceQuota",
                    "application-autoscaling:DescribeScalableTargets",
                ),
                "OneDdb": {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": "dynamodb:GetItem",  # str, not list
                        "Resource": "*",
                    }],
                },
            },
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("DynamoDB" in w for w in warnings)

    def test_deny_statement_does_not_grant_action(self):
        # A Deny on pricing must NOT count as granting it — Deny is ignored
        # by the collector, so pricing stays unsatisfied and warns.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"Mixed": {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Deny", "Action": "pricing:GetProducts", "Resource": "*"},
                    {"Effect": "Allow", "Action": [
                        "servicequotas:GetServiceQuota",
                        "servicequotas:GetAWSDefaultServiceQuota",
                        "application-autoscaling:DescribeScalableTargets",
                    ], "Resource": "*"},
                ],
            }},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("pricing:GetProducts" in w for w in warnings)


class TestMultipleWarnings:
    def test_all_issues_reported_together(self):
        # Bad name, wrong trust, no policies at all.
        iam = _FakeIam(trust=_trust("ec2.amazonaws.com"), attached_arns=[], inline={})
        warnings = validate_custom_role_permissions(iam, "MyCustomRole")
        # name + trust + baseline + pricing + quota + autoscaling + dynamodb
        assert len(warnings) == 7


class TestGracefulDegradation:
    def test_get_role_failure_returns_prefix_check_only(self):
        # get_role fails after the (already-computed) name-prefix check.
        iam = _FakeIam(trust=_trust("glue.amazonaws.com"), fail_on={"get_role"})
        warnings = validate_custom_role_permissions(iam, "MyCustomRole")
        # Only the deterministic name-prefix warning survives; no crash.
        assert warnings == [w for w in warnings if "does not start" in w]
        assert all("does not start" in w for w in warnings)

    def test_get_role_failure_with_good_name_returns_empty(self):
        iam = _FakeIam(trust=_trust("glue.amazonaws.com"), fail_on={"get_role"})
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert warnings == []

    def test_list_attached_failure_keeps_trust_check(self):
        iam = _FakeIam(
            trust=_trust("ec2.amazonaws.com"),
            fail_on={"list_attached_role_policies"},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        # Trust check ran (bad principal → warning); capability checks skipped.
        assert any("glue.amazonaws.com" in w for w in warnings)
        assert not any("pricing" in w for w in warnings)

    def test_policy_document_read_failure_skips_capability_checks(self):
        # Baseline present + reads of policy docs fail → skip 4-7, no false-warn.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            fail_on={"get_policy"},
        )
        warnings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in w for w in warnings)
        assert not any("DynamoDB" in w for w in warnings)
        assert not any("servicequotas" in w.lower() for w in warnings)


class TestIntegrationWithBootstrap:
    """_get_role_name calls the validator and logs each warning for custom roles."""

    def _make_instance(self):
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients
            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            return BootstrapInfrastructure(env)

    def test_custom_role_triggers_validation_and_logs_warnings(self):
        instance = self._make_instance()
        instance._is_existing_role = MagicMock(return_value=True)
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=["Role trust policy does not include glue.amazonaws.com"],
        ) as mock_validate:
            with patch('infrastructure.bootstrap.log') as mock_log:
                instance._get_role_name({'XRole': 'AWSGlueServiceRole-Custom'})
                mock_validate.assert_called_once_with(
                    instance.iam_client, 'AWSGlueServiceRole-Custom'
                )
                mock_log.warning.assert_called()
                assert "glue.amazonaws.com" in mock_log.warning.call_args[0][0]

    def test_standard_role_does_not_trigger_validation(self):
        instance = self._make_instance()
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions'
        ) as mock_validate:
            instance._get_role_name({'XRole': 'READ-ONLY'})
            mock_validate.assert_not_called()

    def test_custom_role_with_no_warnings_logs_nothing(self):
        instance = self._make_instance()
        instance._is_existing_role = MagicMock(return_value=True)
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=[],
        ):
            with patch('infrastructure.bootstrap.log') as mock_log:
                instance._get_role_name({'XRole': 'AWSGlueServiceRole-Custom'})
                mock_log.warning.assert_not_called()
