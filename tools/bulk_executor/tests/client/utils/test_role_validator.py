"""Unit tests for utils.role_validator — custom role permission findings.

When users pass --XRole with a custom IAM role name, validate_custom_role_permissions
checks it against the README's documented minimums for a custom Glue role and
returns a list of Finding(severity, message):

  1. name starts with AWSGlueServiceRole          -- FATAL
  2. trust policy allows glue.amazonaws.com        -- FATAL
  3. AWSGlueServiceRole managed policy attached    -- WARNING
  4. pricing:GetProducts                           -- WARNING (via SimulatePrincipalPolicy)
  5. application-autoscaling:DescribeScalableTargets   -- WARNING, soft (via SimulatePrincipalPolicy)
  6. servicequotas Get{Service,AWSDefaultService}Quota -- WARNING (presence check over Allow statements)
  7. some DynamoDB access                          -- WARNING (presence check over Allow statements)

Two severities, two consequences (see role_validator docstring):

* FATAL is reserved for conditions that are certain and carry zero false-block
  risk -- a role name the bootstrap passrole grant can't cover, and a trust
  policy that won't let Glue assume the role. The caller aborts bootstrap on
  any FATAL finding (tested in TestIntegrationWithBootstrap).
* WARNING is advisory and never blocks -- a locked-down-but-valid role must not
  be rejected, which is the whole reason this validator exists.

How effective permissions are determined (and what these tests pin down):

* pricing / autoscaling are ``Resource:"*"`` actions (neither supports resource
  scoping), so they are evaluated with ``iam:SimulatePrincipalPolicy`` -- IAM's
  own evaluator, which honors Deny / NotAction / conditions.
  ``_FakeIam.simulate_principal_policy`` models that faithfully: it collects
  Allow and Deny patterns from the role's managed + inline documents and lets
  Deny win, so ``TestSimulateHonorsDeny`` proves an explicit Deny suppresses a
  capability the old Allow-only collector would have wrongly counted as granted.
* Service Quotas and DynamoDB are resource-scoped (bootstrap grants the quota
  reads on ``arn:aws:servicequotas:*:*:dynamodb/*`` and DynamoDB on the target
  tables, unknown at bootstrap time), so both are *presence* checks over the
  role's Allow statements. Simulating either against ``*`` would false-deny the
  scoped grant our own bootstrap creates. ``TestQuotaCapability`` (incl. the
  scoped-ARN regression) and ``TestDynamoDbCapability`` cover this.

Every IAM read is best-effort: simulate failing skips only the star
capabilities (pricing/autoscaling); a policy-document read failing skips both
presence checks (quotas + DynamoDB) (``TestGracefulDegradation``).
"""

from unittest.mock import MagicMock, patch

import pytest

from utils.role_validator import (
    FATAL,
    WARNING,
    Finding,
    validate_custom_role_permissions,
)

GLUE_BASELINE_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
DDB_READONLY_ARN = "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"
DDB_FULL_ARN = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
PRICING_FULL_ARN = "arn:aws:iam::aws:policy/AWSPriceListServiceFullAccess"
QUOTAS_RO_ARN = "arn:aws:iam::aws:policy/ServiceQuotasReadOnlyAccess"

GOOD_ROLE_NAME = "AWSGlueServiceRole-MyCustom"


def _msgs(findings):
    """The message strings of a findings list (assertion convenience)."""
    return [f.message for f in findings]


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


def _action_matches(pattern: str, action: str) -> bool:
    """IAM-style action match: '*' matches all, 'svc:*' is a prefix match,
    otherwise an exact (case-insensitive) match. Used by the fake simulator."""
    pattern = pattern.lower()
    action = action.lower()
    if pattern == "*":
        return True
    if pattern.endswith("*"):
        return action.startswith(pattern[:-1])
    return pattern == action


class _FakeIam:
    """Minimal IAM stub driven by declarative role state.

    Models get_role, list_attached_role_policies, get_policy(_version),
    list_role_policies, get_role_policy AND simulate_principal_policy well
    enough to exercise the validator's real read + simulate paths -- instead of
    hand-feeding one canned response. simulate_principal_policy computes its
    decision from the same managed + inline documents (Allow patterns minus Deny
    patterns, Deny wins), so it faithfully honors an explicit Deny.
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
        return {"Role": {
            "Arn": f"arn:aws:iam::123456789012:role/{RoleName}",
            "AssumeRolePolicyDocument": self._trust,
        }}

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

    # --- effective-permission model for SimulatePrincipalPolicy --------------

    def _allow_deny_patterns(self):
        """Collect (allow_patterns, deny_patterns) across managed + inline."""
        allow: list[str] = []
        deny: list[str] = []
        docs = [
            _managed_doc(*self._managed.get(arn, ["*"])) for arn in self._attached
        ] + list(self._inline.values())
        for doc in docs:
            statements = doc.get("Statement", [])
            if isinstance(statements, dict):
                statements = [statements]
            for stmt in statements:
                acts = stmt.get("Action", [])
                if isinstance(acts, str):
                    acts = [acts]
                (allow if stmt.get("Effect") == "Allow" else deny).extend(acts)
        return allow, deny

    def simulate_principal_policy(self, PolicySourceArn, ActionNames):
        self._maybe_fail("simulate_principal_policy")
        allow, deny = self._allow_deny_patterns()
        results = []
        for action in ActionNames:
            denied = any(_action_matches(p, action) for p in deny)
            allowed = any(_action_matches(p, action) for p in allow)
            if allowed and not denied:
                decision = "allowed"
            elif denied:
                decision = "explicitDeny"
            else:
                decision = "implicitDeny"
            results.append({"EvalActionName": action, "EvalDecision": decision})
        return {"EvaluationResults": results}


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
    def test_no_findings_when_all_requirements_met(self):
        findings = validate_custom_role_permissions(_fully_valid_iam(), GOOD_ROLE_NAME)
        assert findings == []


class TestNamePrefix:
    def test_fatal_when_name_lacks_glue_prefix(self):
        iam = _fully_valid_iam()
        findings = validate_custom_role_permissions(iam, "MyCustomRole")
        prefix = [f for f in findings if "does not start" in f.message]
        assert prefix, "expected a name-prefix finding"
        assert all(f.severity == FATAL for f in prefix), (
            "a wrong name prefix guarantees the passrole grant can't cover the "
            "role, so it must be FATAL"
        )
        assert any("AWSGlueServiceRole" in f.message for f in prefix)

    def test_no_prefix_finding_when_name_has_prefix(self):
        iam = _fully_valid_iam()
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("does not start" in m for m in _msgs(findings))


class TestTrustPolicy:
    def test_fatal_when_trust_missing_glue_principal(self):
        iam = _FakeIam(
            trust=_trust("ec2.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts", "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        trust = [f for f in findings if "glue.amazonaws.com" in f.message]
        assert trust, "expected a trust-policy finding"
        assert all(f.severity == FATAL for f in trust), (
            "if Glue can't assume the role the job can't run, so it must be FATAL"
        )

    def test_no_trust_finding_when_glue_in_principal_list(self):
        iam = _FakeIam(
            trust=_trust("lambda.amazonaws.com", "glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts", "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("glue.amazonaws.com" in m for m in _msgs(findings))


class TestGlueBaselinePolicy:
    def test_warns_when_baseline_managed_policy_missing(self):
        iam = _fully_valid_iam()
        iam._attached = [DDB_READONLY_ARN]  # drop the baseline policy
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        baseline = [f for f in findings if "AWSGlueServiceRole managed policy" in f.message]
        assert baseline, "expected a baseline-policy finding"
        assert all(f.severity == WARNING for f in baseline)


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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("pricing:GetProducts" in m for m in _msgs(findings))

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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in m for m in _msgs(findings))

    def test_pricing_via_inline_lockdown_satisfies(self):
        # The documented "maximum lockdown" path: inline pricing:GetProducts.
        iam = _fully_valid_iam()
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in m for m in _msgs(findings))


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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("servicequotas" in m.lower() for m in _msgs(findings))

    def test_quota_via_managed_policy_satisfies(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN, QUOTAS_RO_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("Service Quotas" in m for m in _msgs(findings))

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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("servicequotas" in m.lower() for m in _msgs(findings))

    def test_quota_scoped_to_dynamodb_resource_does_not_warn(self):
        """Regression: bootstrap grants the two quota reads scoped to
        arn:aws:servicequotas:*:*:dynamodb/* (NOT Resource "*"). The old
        SimulatePrincipalPolicy-on-"*" check false-denied that -- warning that a
        role our own bootstrap created lacked quota access. The presence check
        must accept the scoped grant."""
        scoped_quota_stmt = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "servicequotas:GetServiceQuota",
                    "servicequotas:GetAWSDefaultServiceQuota",
                ],
                "Resource": "arn:aws:servicequotas:*:*:dynamodb/*",
            }],
        }
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={
                "star": _managed_doc(
                    "pricing:GetProducts",
                    "application-autoscaling:DescribeScalableTargets",
                ),
                "scopedquota": scoped_quota_stmt,
            },
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("servicequotas" in m.lower() for m in _msgs(findings)), (
            "a DynamoDB-scoped quota grant (what bootstrap creates) must not warn"
        )

    def test_quota_via_servicequotas_service_wildcard_satisfies(self):
        """A servicequotas:* wildcard grant covers both required reads."""
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:*",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("servicequotas" in m.lower() for m in _msgs(findings))

    def test_quota_via_get_prefix_wildcard_satisfies(self):
        """A servicequotas:Get* prefix glob (as ServiceQuotasReadOnlyAccess-style
        grants use) covers both GetServiceQuota and GetAWSDefaultServiceQuota."""
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"x": _managed_doc(
                "pricing:GetProducts",
                "servicequotas:Get*",
                "application-autoscaling:DescribeScalableTargets",
            )},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("servicequotas" in m.lower() for m in _msgs(findings))


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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        autoscaling = [f for f in findings if "application-autoscaling" in f.message]
        assert autoscaling, "expected an autoscaling finding"
        assert all(f.severity == WARNING for f in autoscaling), (
            "autoscaling is a soft requirement -- WARNING, never FATAL"
        )
        assert any("still runs" in f.message for f in autoscaling), (
            "autoscaling finding should note the job still runs (soft requirement)"
        )


class TestSimulateHonorsDeny:
    """The point of moving to SimulatePrincipalPolicy: an Allow cancelled by a
    later Deny is NOT effective permission. The old Allow-only collector counted
    it as granted; the simulator correctly reports it denied, so we still warn."""

    def test_explicit_deny_overrides_allow_for_pricing(self):
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],
            inline={"Mixed": {
                "Version": "2012-10-17",
                "Statement": [
                    # Allow pricing, then Deny it -- effective = denied.
                    {"Effect": "Allow", "Action": "pricing:GetProducts", "Resource": "*"},
                    {"Effect": "Deny", "Action": "pricing:GetProducts", "Resource": "*"},
                    {"Effect": "Allow", "Action": [
                        "servicequotas:GetServiceQuota",
                        "servicequotas:GetAWSDefaultServiceQuota",
                        "application-autoscaling:DescribeScalableTargets",
                    ], "Resource": "*"},
                ],
            }},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("pricing:GetProducts" in m for m in _msgs(findings)), (
            "an Allow-then-Deny on pricing must still warn -- this is exactly "
            "what SimulatePrincipalPolicy catches and the old collector missed"
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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("no DynamoDB permissions" in m for m in _msgs(findings))

    def test_readonly_managed_dynamodb_satisfies(self):
        iam = _fully_valid_iam()  # uses DDB_READONLY_ARN
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))

    def test_restrictive_inline_dynamodb_satisfies(self):
        # README blesses "a more restrictive policy targeting specific tables".
        # Simulating dynamodb against "*" would wrongly deny this; the presence
        # check over Allow statements must accept it.
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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))


class TestPolicyDocumentShapes:
    """The DynamoDB collector must handle the shapes real IAM documents take:
    a single statement (dict, not list), a single action (str, not list),
    Deny statements (ignored), and a full '*' wildcard (grants everything)."""

    def test_admin_star_wildcard_satisfies_every_capability(self):
        # A single inline statement granting "*" satisfies pricing, quota,
        # autoscaling (via simulate) and DynamoDB (via presence) all at once.
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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert findings == [], f"'*' should satisfy everything; got {findings!r}"

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
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))

    def test_deny_dynamodb_statement_is_ignored_by_presence_collector(self):
        # The DynamoDB presence collector only reads Allow statements; a Deny
        # does not count as granting access. Here the only dynamodb mention is a
        # Deny, so the presence check finds nothing and warns.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN],
            inline={"Mixed": {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*"},
                    {"Effect": "Allow", "Action": [
                        "pricing:GetProducts",
                        "servicequotas:GetServiceQuota",
                        "servicequotas:GetAWSDefaultServiceQuota",
                        "application-autoscaling:DescribeScalableTargets",
                    ], "Resource": "*"},
                ],
            }},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("no DynamoDB permissions" in m for m in _msgs(findings))


class TestMultipleFindings:
    def test_all_issues_reported_together(self):
        # Bad name, wrong trust, no policies at all.
        iam = _FakeIam(trust=_trust("ec2.amazonaws.com"), attached_arns=[], inline={})
        findings = validate_custom_role_permissions(iam, "MyCustomRole")
        # name(FATAL) + trust(FATAL) + baseline + pricing + quota + autoscaling + dynamodb
        assert len(findings) == 7
        # Exactly the two certain-failure conditions are FATAL; the rest advise.
        fatals = [f for f in findings if f.severity == FATAL]
        assert len(fatals) == 2
        assert any("does not start" in f.message for f in fatals)
        assert any("glue.amazonaws.com" in f.message for f in fatals)


class TestGracefulDegradation:
    def test_get_role_failure_returns_prefix_check_only(self):
        # get_role fails after the (already-computed) name-prefix check.
        iam = _FakeIam(trust=_trust("glue.amazonaws.com"), fail_on={"get_role"})
        findings = validate_custom_role_permissions(iam, "MyCustomRole")
        # Only the deterministic name-prefix finding survives; no crash.
        assert all("does not start" in f.message for f in findings)
        assert findings, "the no-API name-prefix check should still fire"

    def test_get_role_failure_with_good_name_returns_empty(self):
        iam = _FakeIam(trust=_trust("glue.amazonaws.com"), fail_on={"get_role"})
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert findings == []

    def test_list_attached_failure_keeps_trust_check(self):
        iam = _FakeIam(
            trust=_trust("ec2.amazonaws.com"),
            fail_on={"list_attached_role_policies"},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        # Trust check ran (bad principal → FATAL); capability checks skipped.
        assert any("glue.amazonaws.com" in m for m in _msgs(findings))
        assert not any("pricing" in m for m in _msgs(findings))

    def test_simulate_failure_skips_only_star_capabilities(self):
        # simulate fails → pricing/autoscaling (the star caps) skipped; quotas
        # and DynamoDB are doc-presence checks, independent of simulate, and
        # satisfied here, so NO findings at all.
        iam = _fully_valid_iam()
        iam._fail_on = {"simulate_principal_policy"}
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in m for m in _msgs(findings))
        assert not any("servicequotas" in m.lower() for m in _msgs(findings))
        assert not any("application-autoscaling" in m for m in _msgs(findings))
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))

    def test_simulate_failure_does_not_skip_quota_presence_check(self):
        # Regression for the mechanism change: quotas is now a presence check,
        # so a simulate failure must NOT suppress it. A role missing quota
        # access still warns even when SimulatePrincipalPolicy is unavailable.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN],  # no quota access
            fail_on={"simulate_principal_policy"},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert any("servicequotas" in m.lower() for m in _msgs(findings)), (
            "quotas is a presence check; simulate being down must not hide it"
        )

    def test_missing_role_arn_skips_only_star_capabilities(self):
        # Defensive: if get_role returns a role with no Arn, we can't simulate,
        # so the star caps (pricing/autoscaling) are skipped (not false-warned).
        # Quotas + DynamoDB are doc-presence checks, unaffected, satisfied here.
        iam = _fully_valid_iam()
        original = iam.get_role
        iam.get_role = lambda RoleName: {"Role": {
            "AssumeRolePolicyDocument": original(RoleName)["Role"]["AssumeRolePolicyDocument"],
        }}  # no Arn key
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        assert not any("pricing" in m for m in _msgs(findings))
        assert not any("servicequotas" in m.lower() for m in _msgs(findings))
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))

    def test_policy_read_failure_skips_both_presence_checks(self):
        # get_policy fails → the quota AND DynamoDB presence checks are skipped
        # (both read policy docs); the star caps (via simulate) still evaluate.
        # Use a role with NO quota/DynamoDB grant so, if the presence checks
        # wrongly ran on empty data, they would warn — proving they're skipped.
        iam = _FakeIam(
            trust=_trust("glue.amazonaws.com"),
            attached_arns=[GLUE_BASELINE_ARN, DDB_FULL_ARN, QUOTAS_RO_ARN],
            fail_on={"get_policy"},
        )
        findings = validate_custom_role_permissions(iam, GOOD_ROLE_NAME)
        # Both presence checks couldn't run → no false warnings.
        assert not any("no DynamoDB permissions" in m for m in _msgs(findings))
        assert not any("servicequotas" in m.lower() for m in _msgs(findings))


class TestIntegrationWithBootstrap:
    """_get_role_name surfaces findings: WARNINGs are logged, a FATAL aborts."""

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

    def test_custom_role_warning_is_logged_and_does_not_abort(self):
        instance = self._make_instance()
        instance._is_existing_role = MagicMock(return_value=True)
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=[Finding(
                WARNING,
                "Role is missing the AWSGlueServiceRole managed policy.",
            )],
        ) as mock_validate:
            with patch('infrastructure.bootstrap.log') as mock_log:
                result = instance._get_role_name({'XRole': 'AWSGlueServiceRole-Custom'})
                mock_validate.assert_called_once_with(
                    instance.iam_client, 'AWSGlueServiceRole-Custom'
                )
                mock_log.warning.assert_called()
                mock_log.error.assert_not_called()
                assert result == 'AWSGlueServiceRole-Custom'

    def test_custom_role_fatal_finding_aborts_bootstrap(self):
        instance = self._make_instance()
        instance._is_existing_role = MagicMock(return_value=True)
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=[Finding(
                FATAL,
                "Role trust policy does not allow glue.amazonaws.com to assume the role.",
            )],
        ):
            with patch('infrastructure.bootstrap.log') as mock_log:
                with pytest.raises(SystemExit):
                    instance._get_role_name({'XRole': 'AWSGlueServiceRole-Custom'})
                mock_log.error.assert_called()
                assert "glue.amazonaws.com" in mock_log.error.call_args[0][0]

    def test_standard_role_does_not_trigger_validation(self):
        instance = self._make_instance()
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions'
        ) as mock_validate:
            instance._get_role_name({'XRole': 'READ-ONLY'})
            mock_validate.assert_not_called()

    def test_custom_role_with_no_findings_logs_nothing(self):
        instance = self._make_instance()
        instance._is_existing_role = MagicMock(return_value=True)
        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=[],
        ):
            with patch('infrastructure.bootstrap.log') as mock_log:
                instance._get_role_name({'XRole': 'AWSGlueServiceRole-Custom'})
                mock_log.warning.assert_not_called()
                mock_log.error.assert_not_called()
