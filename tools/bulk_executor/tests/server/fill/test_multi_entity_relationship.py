"""Unit tests for the `multi_entity_relationship` fill generator.

Covers `python_modules/fill/multi_entity_relationship.py`:
- generate(): top-level orchestration — returns [company, product, event, *users],
  shares company_id across product/event/users, builds 1 user per UserId in company
- _random_timestamp_millis(): bounds (within last 5 years), millisecond integer
- _random_uuid_list(): default num_uuids=5, configurable count, returned values
  are valid UUID strings, sample size bounded by num_uuids
- _fake_title(): delegates to fake.sentence(nb_words=3, variable_nb_words=True)
- _get_user(): default id/company_ids generation, sk='User', schema fields all
  present, value-domain checks (random.choice/sample inputs)
- _get_company(): default id/user_ids (100 users), sk='Company', schema fields
- _get_product(): default id/company_id, sk='Product', minimal schema
- _get_event(): default id/company_id, sk='Event', includes Date field
- _get_application(): default id/company_id, sk='Application', full schema

The parent tests/server/conftest.py mocks awsglue/pyspark/shared. The fill/
conftest.py additionally stubs faker so this module can be imported.
"""

import re
import time
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from python_modules.fill import multi_entity_relationship as mer


_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
)

# Domains used inside _get_user / _get_company / _get_application — kept here
# so tests don't repeat the literals scattered across the source.
INTERACTION_TYPES = {"Web", "Mobile", "App", "Other"}
AWS_SERVICES = {"Activate", "AppSync", "DynamoDB", "Q", "S3", "SageMaker"}
PRIMARY_CHALLENGES = {
    "Building scalable architecture",
    "Choosing right tech solutions",
    "Operational excellence",
    "Finding co-founders",
    "Finding talent",
}
COMPLIANCE_REQUESTER_TYPES = {
    "Customer", "Investor", "Partner", "Personal",
    "Shareholder", "Supplier", "Vendor",
}
INDUSTRIES = {
    "Automotive", "Biotechnology", "Education", "Entertainment",
    "Hospitality", "Information Technology", "Manufacturing",
    "Pharmaceutical", "Retail", "Telecommunications",
}
ROLES = {
    "Software Engineer", "Project Manager", "Marketing Coordinator",
    "Human Resources Manager", "Financial Analyst", "Graphic Designer",
    "Sales Representative", "Accountant", "Data Scientist",
    "Operations Manager",
}
YEARS_ON_AWS = {
    "Less than one year", "1-2 years", "2-5 years",
    "5-10 years", "10+ years",
}
NUM_EMPLOYEES = {"1 - 10", "10-20", "20-50", "50-100", "100-1000", "1000+"}
FUNDING_ROUNDS = {
    "Bootstrap", "Pre-Seed", "Seed", "Series A", "Series B", "Series C",
}
COMPANY_TYPES = {
    "Cooperative", "Corporation (C-Corp or S-Corp)", "Franchise",
    "Government - Federal", "Government - Local", "Holding Company",
    "Joint Venture", "Limited Liability Company (LLC)",
    "Non-Profit Organization", "Partnership", "Public Company (Publicly Traded)",
    "Sole Proprietorship",
}


# --- _random_timestamp_millis -----------------------------------------------

class TestRandomTimestampMillis:
    """Tests for _random_timestamp_millis() (lines 22-26)."""

    def test_returns_int(self):
        """Line 26: random.randint returns an int."""
        out = mer._random_timestamp_millis()
        assert isinstance(out, int)

    def test_within_last_five_years(self):
        """Line 25: lower bound is now - 5 years in ms; upper is now."""
        before = int(time.time() * 1000)
        out = mer._random_timestamp_millis()
        after = int(time.time() * 1000)
        five_years_ms = 5 * 365 * 24 * 60 * 60 * 1000
        assert (before - five_years_ms) <= out <= after

    def test_distribution_spans_window(self):
        """Across many calls, both ends of the 5-year window should be reachable."""
        samples = [mer._random_timestamp_millis() for _ in range(200)]
        assert min(samples) != max(samples), "values should vary across calls"


# --- _random_uuid_list -------------------------------------------------------

class TestRandomUuidList:
    """Tests for _random_uuid_list() (lines 29-33)."""

    def test_default_num_uuids_is_5(self):
        """Line 29: signature default num_uuids=5 → returned list size 1..5."""
        out = mer._random_uuid_list()
        assert 1 <= len(out) <= 5

    def test_each_element_is_valid_uuid(self):
        """Line 32: uuid.uuid4() values are formatted UUIDs."""
        out = mer._random_uuid_list(num_uuids=5)
        for u in out:
            assert _UUID_RE.match(u), f"{u} should be a valid UUID4 string"

    def test_custom_count_bounds_sample(self):
        """Line 33: random.sample bounded by num_uuids."""
        out = mer._random_uuid_list(num_uuids=10)
        assert 1 <= len(out) <= 10

    def test_zero_count_raises(self):
        """num_uuids=0 → random.randint(1, 0) raises ValueError (documented behavior)."""
        with pytest.raises(ValueError):
            mer._random_uuid_list(num_uuids=0)

    def test_one_count_returns_singleton(self):
        """num_uuids=1 → exactly one UUID."""
        out = mer._random_uuid_list(num_uuids=1)
        assert len(out) == 1
        assert _UUID_RE.match(out[0])


# --- _fake_title -------------------------------------------------------------

class TestFakeTitle:
    """Tests for _fake_title() (lines 36-37)."""

    def test_calls_fake_sentence_with_documented_args(self, monkeypatch):
        """Line 37: fake.sentence(nb_words=3, variable_nb_words=True)."""
        sentence = MagicMock(return_value='Hello world here')
        monkeypatch.setattr(mer.fake, 'sentence', sentence)
        result = mer._fake_title()
        sentence.assert_called_once_with(nb_words=3, variable_nb_words=True)
        assert result == 'Hello world here'

    def test_returns_string(self):
        """The stubbed faker returns str — so does the real one."""
        out = mer._fake_title()
        assert isinstance(out, str)


# --- _get_user ---------------------------------------------------------------

class TestGetUser:
    """Tests for _get_user() (lines 40-170)."""

    def test_default_id_is_uuid(self):
        """Line 42: id defaults to uuid.uuid4()."""
        user = mer._get_user()
        assert _UUID_RE.match(user['pk'])

    def test_explicit_id_used(self):
        """Line 41-42: when id provided, pk == id."""
        user = mer._get_user(id='user-abc')
        assert user['pk'] == 'user-abc'

    def test_default_company_ids_is_random_uuid_list(self):
        """Lines 43-44: company_ids defaults to _random_uuid_list()."""
        user = mer._get_user()
        # default _random_uuid_list returns 1..5 entries, all UUIDs
        assert 1 <= len(user['CompanyIds']) <= 5
        for cid in user['CompanyIds']:
            assert _UUID_RE.match(cid)

    def test_explicit_company_ids_used(self):
        """Line 66: explicit company_ids passes through."""
        user = mer._get_user(company_ids=['c1', 'c2'])
        assert user['CompanyIds'] == ['c1', 'c2']

    def test_sk_is_user(self):
        """Line 48: sk='User'."""
        assert mer._get_user()['sk'] == 'User'

    def test_required_fields_present(self):
        """Lines 46-170: full field set."""
        user = mer._get_user()
        expected_keys = {
            'pk', 'sk', 'About', 'InteractionType', 'AwsServices',
            'PrimaryChallenges', 'City', 'CompanyIds', 'CompanyEmail',
            'HasComplianceOptIn', 'HasMarketingOptIn',
            'ComplianceRequesterType', 'Country', 'CreatedDate',
            'DisplayName', 'FirstName', 'Industries', 'IsBlocklisted',
            'LastName', 'LastAuthenticatedDate', 'LastUpdatedDate',
            'AwsAccountIds', 'IsFounder', 'PhoneNumber', 'Role',
            'StateOrProvince', 'TechnologiesUsed', 'YearsOnAws', 'ZipCode',
        }
        assert expected_keys <= set(user.keys())

    def test_interaction_type_in_domain(self):
        """Line 50: random.choice over INTERACTION_TYPES."""
        for _ in range(10):
            assert mer._get_user()['InteractionType'] in INTERACTION_TYPES

    def test_aws_services_subset_of_domain(self):
        """Lines 51-54: random.sample 1..3 from AWS_SERVICES."""
        for _ in range(10):
            services = mer._get_user()['AwsServices']
            assert 1 <= len(services) <= 3
            assert set(services) <= AWS_SERVICES

    def test_primary_challenges_subset_of_domain(self):
        """Lines 55-64: random.sample 1..3 from PRIMARY_CHALLENGES."""
        for _ in range(10):
            challenges = mer._get_user()['PrimaryChallenges']
            assert 1 <= len(challenges) <= 3
            assert set(challenges) <= PRIMARY_CHALLENGES

    def test_compliance_flags_are_bool(self):
        """Lines 68-69: random.choice([True, False])."""
        user = mer._get_user()
        assert isinstance(user['HasComplianceOptIn'], bool)
        assert isinstance(user['HasMarketingOptIn'], bool)
        assert isinstance(user['IsBlocklisted'], bool)
        assert isinstance(user['IsFounder'], bool)

    def test_compliance_requester_type_in_domain(self):
        """Lines 70-80."""
        for _ in range(10):
            assert mer._get_user()['ComplianceRequesterType'] in COMPLIANCE_REQUESTER_TYPES

    def test_industries_subset_of_domain(self):
        """Lines 85-99: random.sample 1..5 from INDUSTRIES."""
        for _ in range(10):
            inds = mer._get_user()['Industries']
            assert 1 <= len(inds) <= 5
            assert set(inds) <= INDUSTRIES

    def test_aws_account_ids_are_12_digit_strings(self):
        """Lines 104-117: random.randint(1e11, 1e12-1) → 12 digits, set-deduped."""
        for _ in range(5):
            account_ids = mer._get_user()['AwsAccountIds']
            assert 1 <= len(account_ids) <= 5
            for aid in account_ids:
                assert isinstance(aid, str)
                assert len(aid) == 12
                assert aid.isdigit()

    def test_role_in_domain(self):
        """Lines 120-133."""
        for _ in range(10):
            assert mer._get_user()['Role'] in ROLES

    def test_years_on_aws_in_domain(self):
        """Lines 160-168."""
        for _ in range(10):
            assert mer._get_user()['YearsOnAws'] in YEARS_ON_AWS

    def test_timestamps_are_int(self):
        """Lines 82, 102, 103: timestamp fields are int millis."""
        user = mer._get_user()
        assert isinstance(user['CreatedDate'], int)
        assert isinstance(user['LastAuthenticatedDate'], int)
        assert isinstance(user['LastUpdatedDate'], int)


# --- _get_company ------------------------------------------------------------

class TestGetCompany:
    """Tests for _get_company() (lines 173-304)."""

    def test_default_id_is_uuid(self):
        """Line 175: id defaults to uuid.uuid4()."""
        c = mer._get_company()
        assert _UUID_RE.match(c['pk'])

    def test_explicit_id_used(self):
        """Lines 174-175."""
        c = mer._get_company(id='comp-1')
        assert c['pk'] == 'comp-1'

    def test_default_user_ids_is_100_uuid_pool(self):
        """Lines 176-177: default user_ids is _random_uuid_list(100)."""
        c = mer._get_company()
        # _random_uuid_list(100) → random.sample(100 uuids, randint(1, 100)).
        # Resulting UserIds list has at least 1 entry, all valid UUIDs.
        assert 1 <= len(c['UserIds']) <= 100
        for uid in c['UserIds']:
            assert _UUID_RE.match(uid)

    def test_explicit_user_ids_used(self):
        """Line 209."""
        c = mer._get_company(user_ids=['u1', 'u2', 'u3'])
        assert c['UserIds'] == ['u1', 'u2', 'u3']

    def test_sk_is_company(self):
        """Line 181."""
        assert mer._get_company()['sk'] == 'Company'

    def test_funding_round_in_domain(self):
        """Lines 184-193."""
        for _ in range(10):
            assert mer._get_company()['FundingRound'] in FUNDING_ROUNDS

    def test_company_type_in_domain(self):
        """Lines 212-227."""
        for _ in range(10):
            assert mer._get_company()['CompanyType'] in COMPANY_TYPES

    def test_number_of_employees_in_domain(self):
        """Lines 264-266."""
        for _ in range(10):
            assert mer._get_company()['NumberOfEmployees'] in NUM_EMPLOYEES

    def test_required_fields_present(self):
        """Full schema."""
        c = mer._get_company()
        expected = {
            'pk', 'sk', 'About', 'InteractionType', 'FundingRound',
            'AwsServices', 'PrimaryChallenges', 'City', 'UserIds',
            'CompanyName', 'CompanyEmail', 'CompanyType', 'CompanyWebsite',
            'Country', 'CreatedDate', 'CompanyFoundedDate', 'Industries',
            'IsBlocklisted', 'LastUpdatedDate', 'ProductLaunchDate',
            'AwsAccountIds', 'NumberOfEmployees', 'PhoneNumber',
            'StateOrProvince', 'TechnologiesUsed', 'YearsOnAws', 'ZipCode',
        }
        assert expected <= set(c.keys())

    def test_aws_services_subset(self):
        """Lines 194-197."""
        services = mer._get_company()['AwsServices']
        assert 1 <= len(services) <= 3
        assert set(services) <= AWS_SERVICES


# --- _get_product ------------------------------------------------------------

class TestGetProduct:
    """Tests for _get_product() (lines 307-319)."""

    def test_default_id_is_uuid(self):
        """Line 309."""
        p = mer._get_product()
        assert _UUID_RE.match(p['pk'])

    def test_default_company_id_is_uuid(self):
        """Line 311: independent uuid for CompanyId."""
        p = mer._get_product()
        assert _UUID_RE.match(p['CompanyId'])

    def test_explicit_ids_used(self):
        """Lines 308-311."""
        p = mer._get_product(id='prod-1', company_id='comp-1')
        assert p['pk'] == 'prod-1'
        assert p['CompanyId'] == 'comp-1'

    def test_sk_is_product(self):
        """Line 315."""
        assert mer._get_product()['sk'] == 'Product'

    def test_schema_keys_minimal(self):
        """Lines 313-319: pk, sk, CompanyId, Title, Description only."""
        p = mer._get_product()
        assert set(p.keys()) == {'pk', 'sk', 'CompanyId', 'Title', 'Description'}

    def test_title_calls_fake_title(self, monkeypatch):
        """Line 317: Title = _fake_title()."""
        monkeypatch.setattr(mer, '_fake_title', lambda: 'Stub Title')
        p = mer._get_product()
        assert p['Title'] == 'Stub Title'


# --- _get_event --------------------------------------------------------------

class TestGetEvent:
    """Tests for _get_event() (lines 322-335)."""

    def test_default_id_is_uuid(self):
        """Line 324."""
        e = mer._get_event()
        assert _UUID_RE.match(e['pk'])

    def test_default_company_id_is_uuid(self):
        """Line 326."""
        e = mer._get_event()
        assert _UUID_RE.match(e['CompanyId'])

    def test_explicit_ids_used(self):
        """Lines 323-326."""
        e = mer._get_event(id='evt-1', company_id='comp-9')
        assert e['pk'] == 'evt-1'
        assert e['CompanyId'] == 'comp-9'

    def test_sk_is_event(self):
        """Line 330."""
        assert mer._get_event()['sk'] == 'Event'

    def test_includes_date_field_int_millis(self):
        """Line 334: Date = _random_timestamp_millis()."""
        e = mer._get_event()
        assert 'Date' in e
        assert isinstance(e['Date'], int)

    def test_schema_keys(self):
        """Lines 328-335: pk, sk, CompanyId, Title, Description, Date."""
        e = mer._get_event()
        assert set(e.keys()) == {'pk', 'sk', 'CompanyId', 'Title', 'Description', 'Date'}


# --- _get_application --------------------------------------------------------

class TestGetApplication:
    """Tests for _get_application() (lines 339-502)."""

    def test_default_id_is_uuid(self):
        """Line 341."""
        a = mer._get_application()
        assert _UUID_RE.match(a['pk'])

    def test_default_company_id_is_uuid(self):
        """Line 343."""
        a = mer._get_application()
        assert _UUID_RE.match(a['CompanyId'])

    def test_explicit_ids_used(self):
        """Lines 340-343."""
        a = mer._get_application(id='app-1', company_id='comp-z')
        assert a['pk'] == 'app-1'
        assert a['CompanyId'] == 'comp-z'

    def test_sk_is_application(self):
        """Line 347."""
        assert mer._get_application()['sk'] == 'Application'

    def test_funding_round_in_domain(self):
        """Lines 350-358."""
        for _ in range(10):
            assert mer._get_application()['FundingRound'] in FUNDING_ROUNDS

    def test_compliance_flags_are_bool(self):
        """Lines 395-396."""
        a = mer._get_application()
        assert isinstance(a['HasComplianceOptIn'], bool)
        assert isinstance(a['HasMarketingOptIn'], bool)

    def test_role_in_domain(self):
        """Lines 452-465."""
        for _ in range(10):
            assert mer._get_application()['Role'] in ROLES

    def test_aws_account_ids_are_12_digit_strings(self):
        """Lines 433-446."""
        for _ in range(5):
            ids = mer._get_application()['AwsAccountIds']
            assert 1 <= len(ids) <= 5
            for aid in ids:
                assert len(aid) == 12 and aid.isdigit()

    def test_required_fields_present(self):
        """Full schema (a superset of user + company-ish fields)."""
        a = mer._get_application()
        expected = {
            'pk', 'sk', 'About', 'InteractionType', 'FundingRound',
            'AwsServices', 'PrimaryChallenges', 'City', 'CompanyId',
            'CompanyName', 'CompanyEmail', 'CompanyType', 'CompanyWebsite',
            'HasComplianceOptIn', 'HasMarketingOptIn',
            'ComplianceRequesterType', 'Country', 'CreatedDate',
            'DisplayName', 'FirstName', 'CompanyFoundedDate', 'Industries',
            'IsBlocklisted', 'LastName', 'LastAuthenticatedDate',
            'LastUpdatedDate', 'ProductLaunchDate', 'AwsAccountIds',
            'NumberOfEmployees', 'IsFounder', 'PhoneNumber', 'Role',
            'StateOrProvince', 'TechnologiesUsed', 'YearsOnAws', 'ZipCode',
        }
        assert expected <= set(a.keys())


# --- generate (orchestrator) -------------------------------------------------

class TestGenerate:
    """Tests for the top-level generate() (lines 10-19)."""

    def test_returns_at_least_company_product_event(self, monkeypatch):
        """Line 19: returns [company, product, event] + users."""
        # Force a deterministic 1-user company so we can index reliably.
        company = {
            'pk': 'company-1', 'sk': 'Company', 'UserIds': ['user-1'],
        }
        monkeypatch.setattr(mer, '_get_company', lambda: company)
        monkeypatch.setattr(mer, '_get_user',
                            lambda user_id, company_ids: {
                                'pk': user_id, 'sk': 'User',
                                'CompanyIds': company_ids,
                            })
        monkeypatch.setattr(mer, '_get_product',
                            lambda company_id: {
                                'pk': 'product-1', 'sk': 'Product',
                                'CompanyId': company_id,
                            })
        monkeypatch.setattr(mer, '_get_event',
                            lambda company_id: {
                                'pk': 'event-1', 'sk': 'Event',
                                'CompanyId': company_id,
                            })
        # _random_uuid_list still real — controls the "extra" company_ids on user
        out = mer.generate()
        assert len(out) == 4, "company + product + event + 1 user"
        assert out[0] is company
        assert out[1]['sk'] == 'Product'
        assert out[2]['sk'] == 'Event'
        assert out[3]['sk'] == 'User'

    def test_company_id_propagates_to_product_and_event(self, monkeypatch):
        """Lines 13, 17-18: company_id passed to _get_product and _get_event."""
        company = {
            'pk': 'company-X', 'sk': 'Company', 'UserIds': [],
        }
        monkeypatch.setattr(mer, '_get_company', lambda: company)
        monkeypatch.setattr(mer, '_get_product',
                            lambda company_id: {'sk': 'Product', 'CompanyId': company_id})
        monkeypatch.setattr(mer, '_get_event',
                            lambda company_id: {'sk': 'Event', 'CompanyId': company_id})

        out = mer.generate()
        product = next(x for x in out if x['sk'] == 'Product')
        event = next(x for x in out if x['sk'] == 'Event')
        assert product['CompanyId'] == 'company-X'
        assert event['CompanyId'] == 'company-X'

    def test_one_user_per_user_id_in_company(self, monkeypatch):
        """Lines 14-16: a user is created for each id in company.UserIds."""
        company = {
            'pk': 'company-Y', 'sk': 'Company',
            'UserIds': ['u1', 'u2', 'u3', 'u4'],
        }
        monkeypatch.setattr(mer, '_get_company', lambda: company)
        seen_user_ids = []

        def fake_user(user_id, company_ids):
            seen_user_ids.append(user_id)
            return {'pk': user_id, 'sk': 'User', 'CompanyIds': company_ids}

        monkeypatch.setattr(mer, '_get_user', fake_user)
        monkeypatch.setattr(mer, '_get_product', lambda company_id: {'sk': 'Product'})
        monkeypatch.setattr(mer, '_get_event', lambda company_id: {'sk': 'Event'})

        out = mer.generate()
        users = [x for x in out if x.get('sk') == 'User']
        assert len(users) == 4
        assert seen_user_ids == ['u1', 'u2', 'u3', 'u4']

    def test_each_user_company_ids_starts_with_company_pk(self, monkeypatch):
        """Line 15: company_ids passed = [company_id] + _random_uuid_list()."""
        company = {
            'pk': 'company-Z', 'sk': 'Company', 'UserIds': ['u1'],
        }
        monkeypatch.setattr(mer, '_get_company', lambda: company)
        captured = {}

        def fake_user(user_id, company_ids):
            captured['company_ids'] = company_ids
            return {'pk': user_id, 'sk': 'User', 'CompanyIds': company_ids}

        monkeypatch.setattr(mer, '_get_user', fake_user)
        monkeypatch.setattr(mer, '_get_product', lambda company_id: {'sk': 'Product'})
        monkeypatch.setattr(mer, '_get_event', lambda company_id: {'sk': 'Event'})

        mer.generate()
        assert captured['company_ids'][0] == 'company-Z', \
            "company.pk should be the first entry in user.CompanyIds"
        assert len(captured['company_ids']) >= 2, \
            "additional random uuids should be appended"

    def test_returns_a_list(self):
        """Line 19: end-to-end smoke — output is a list of dicts."""
        out = mer.generate()
        assert isinstance(out, list)
        assert all(isinstance(x, dict) for x in out)
        # company + product + event + at least 1 user (UserIds always >= 1
        # because _random_uuid_list returns 1..N)
        assert len(out) >= 4

    def test_smoke_real_dependencies(self):
        """End-to-end with no monkeypatching — exercises every helper for
        coverage of the real branches inside _get_company, _get_user, etc."""
        for _ in range(3):
            out = mer.generate()
            sks = [item.get('sk') for item in out]
            assert 'Company' in sks
            assert 'Product' in sks
            assert 'Event' in sks
            assert 'User' in sks
