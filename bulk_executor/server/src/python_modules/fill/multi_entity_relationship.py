import random
import time
import uuid

from faker import Faker

fake = Faker()


def generate():
    users = []
    company = _get_company()
    company_id = company.get("pk")
    for user_id in company.get("UserIds"):
        user = _get_user(user_id, [company_id] + _random_uuid_list())
        users.append(user)
    product = _get_product(company_id=company_id)
    event = _get_event(company_id=company_id)
    return [company, product, event] + users


def _random_timestamp_millis():
    now_ms = int(time.time() * 1000)
    # Keep Timestamps within the past 5 years
    five_years_ago_ms = now_ms - (5 * 365 * 24 * 60 * 60 * 1000)
    return random.randint(five_years_ago_ms, now_ms)


def _random_uuid_list(num_uuids=5):
    uuids = []
    for _ in range(num_uuids):
        uuids.append(str(uuid.uuid4()))
    return random.sample(uuids, random.randint(1, num_uuids))


def _fake_title():
    return fake.sentence(nb_words=3, variable_nb_words=True)


def _get_user(id=None, company_ids=None):
    if id is None:
        id = str(uuid.uuid4())
    if company_ids is None:
        company_ids = _random_uuid_list()

    return {
        "pk": id,
        "sk": "User",
        "About": fake.paragraph(),
        "InteractionType": random.choice(["Web", "Mobile", "App", "Other"]),
        "AwsServices": random.sample(
            ["Activate", "AppSync", "DynamoDB", "Q", "S3", "SageMaker"],
            random.randint(1, 3),
        ),
        "PrimaryChallenges": random.sample(
            [
                "Building scalable architecture",
                "Choosing right tech solutions",
                "Operational excellence",
                "Finding co-founders",
                "Finding talent",
            ],
            random.randint(1, 3),
        ),
        "City": fake.city(),
        "CompanyIds": company_ids,
        "CompanyEmail": fake.email(),
        "HasComplianceOptIn": random.choice([True, False]),
        "HasMarketingOptIn": random.choice([True, False]),
        "ComplianceRequesterType": random.choice(
            [
                "Customer",
                "Investor",
                "Partner",
                "Personal",
                "Shareholder",
                "Supplier",
                "Vendor",
            ]
        ),
        "Country": fake.country(),
        "CreatedDate": _random_timestamp_millis(),
        "DisplayName": fake.user_name(),
        "FirstName": fake.first_name(),
        "Industries": random.sample(
            [
                "Automotive",
                "Biotechnology",
                "Education",
                "Entertainment",
                "Hospitality",
                "Information Technology",
                "Manufacturing",
                "Pharmaceutical",
                "Retail",
                "Telecommunications",
            ],
            random.randint(1, 5),
        ),
        "IsBlocklisted": random.choice([True, False]),
        "LastName": fake.last_name(),
        "LastAuthenticatedDate": _random_timestamp_millis(),
        "LastUpdatedDate": _random_timestamp_millis(),
        "AwsAccountIds": random.sample(
            list(
                set(
                    [
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                    ]
                )
            ),
            random.randint(1, 5),
        ),
        "IsFounder": random.choice([True, False]),
        "PhoneNumber": fake.phone_number(),
        "Role": random.choice(
            [
                "Software Engineer",
                "Project Manager",
                "Marketing Coordinator",
                "Human Resources Manager",
                "Financial Analyst",
                "Graphic Designer",
                "Sales Representative",
                "Accountant",
                "Data Scientist",
                "Operations Manager",
            ]
        ),
        "StateOrProvince": fake.state(),
        "TechnologiesUsed": random.sample(
            [
                "Customer Relationship Management (CRM) software",
                "Enterprise Resource Planning (ERP) system",
                "Cloud computing services (e.g., Amazon Web Services, Microsoft Azure, Google Cloud)",
                "Content Management System (CMS) (e.g., WordPress, Drupal, Joomla)",
                "E-commerce platforms (e.g., Shopify, WooCommerce, Magento)",
                "Project management tools (e.g., Asana, Trello, Jira)",
                "Collaboration and communication tools (e.g., Slack, Microsoft Teams, Zoom)",
                "Business intelligence and analytics tools (e.g., Tableau, Power BI, QlikView)",
                "Cybersecurity solutions (e.g., firewalls, antivirus software, VPNs)",
                "Human Resources Management System (HRMS)",
                "Marketing automation platforms (e.g., HubSpot, Marketo, Pardot)",
                "Social media management tools (e.g., Hootsuite, Sprout Social, Buffer)",
                "Accounting and financial software (e.g., QuickBooks, Xero, FreshBooks)",
                "Inventory management systems",
                "Supply chain management software",
                "Customer support and ticketing systems (e.g., Zendesk, Freshdesk)",
                "Document management systems",
                "Virtual meeting and webinar platforms (e.g., GoToMeeting, WebEx)",
                "Email marketing platforms (e.g., Mailchimp, Constant Contact)",
                "Learning Management Systems (LMS) for employee training",
            ],
            random.randint(1, 5),
        ),
        "YearsOnAws": random.choice(
            [
                "Less than one year",
                "1-2 years",
                "2-5 years",
                "5-10 years",
                "10+ years",
            ]
        ),
        "ZipCode": fake.postcode(),
    }


def _get_company(id=None, user_ids=None):
    if id is None:
        id = str(uuid.uuid4())
    if user_ids is None:
        user_ids = _random_uuid_list(100)

    return {
        "pk": id,
        "sk": "Company",
        "About": fake.paragraph(),
        "InteractionType": random.choice(["Web", "Mobile", "App", "Other"]),
        "FundingRound": random.choice(
            [
                "Bootstrap",
                "Pre-Seed",
                "Seed",
                "Series A",
                "Series B",
                "Series C",
            ]
        ),
        "AwsServices": random.sample(
            ["Activate", "AppSync", "DynamoDB", "Q", "S3", "SageMaker"],
            random.randint(1, 3),
        ),
        "PrimaryChallenges": random.sample(
            [
                "Building scalable architecture",
                "Choosing right tech solutions",
                "Operational excellence",
                "Finding co-founders",
                "Finding talent",
            ],
            random.randint(1, 3),
        ),
        "City": fake.city(),
        "UserIds": user_ids,
        "CompanyName": fake.company(),
        "CompanyEmail": fake.email(),
        "CompanyType": random.choice(
            [
                "Cooperative",
                "Corporation (C-Corp or S-Corp)",
                "Franchise",
                "Government - Federal",
                "Government - Local",
                "Holding Company",
                "Joint Venture",
                "Limited Liability Company (LLC)",
                "Non-Profit Organization",
                "Partnership",
                "Public Company (Publicly Traded)",
                "Sole Proprietorship",
            ]
        ),
        "CompanyWebsite": fake.domain_name(),
        "Country": fake.country(),
        "CreatedDate": _random_timestamp_millis(),
        "CompanyFoundedDate": _random_timestamp_millis(),
        "Industries": random.sample(
            [
                "Automotive",
                "Biotechnology",
                "Education",
                "Entertainment",
                "Hospitality",
                "Information Technology",
                "Manufacturing",
                "Pharmaceutical",
                "Retail",
                "Telecommunications",
            ],
            random.randint(1, 5),
        ),
        "IsBlocklisted": random.choice([True, False]),
        "LastUpdatedDate": _random_timestamp_millis(),
        "ProductLaunchDate": _random_timestamp_millis(),
        "AwsAccountIds": random.sample(
            list(
                set(
                    [
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                    ]
                )
            ),
            random.randint(1, 5),
        ),
        "NumberOfEmployees": random.choice(
            ["1 - 10", "10-20", "20-50", "50-100", "100-1000", "1000+"]
        ),
        "PhoneNumber": fake.phone_number(),
        "StateOrProvince": fake.state(),
        "TechnologiesUsed": random.sample(
            [
                "Customer Relationship Management (CRM) software",
                "Enterprise Resource Planning (ERP) system",
                "Cloud computing services (e.g., Amazon Web Services, Microsoft Azure, Google Cloud)",
                "Content Management System (CMS) (e.g., WordPress, Drupal, Joomla)",
                "E-commerce platforms (e.g., Shopify, WooCommerce, Magento)",
                "Project management tools (e.g., Asana, Trello, Jira)",
                "Collaboration and communication tools (e.g., Slack, Microsoft Teams, Zoom)",
                "Business intelligence and analytics tools (e.g., Tableau, Power BI, QlikView)",
                "Cybersecurity solutions (e.g., firewalls, antivirus software, VPNs)",
                "Human Resources Management System (HRMS)",
                "Marketing automation platforms (e.g., HubSpot, Marketo, Pardot)",
                "Social media management tools (e.g., Hootsuite, Sprout Social, Buffer)",
                "Accounting and financial software (e.g., QuickBooks, Xero, FreshBooks)",
                "Inventory management systems",
                "Supply chain management software",
                "Customer support and ticketing systems (e.g., Zendesk, Freshdesk)",
                "Document management systems",
                "Virtual meeting and webinar platforms (e.g., GoToMeeting, WebEx)",
                "Email marketing platforms (e.g., Mailchimp, Constant Contact)",
                "Learning Management Systems (LMS) for employee training",
            ],
            random.randint(1, 5),
        ),
        "YearsOnAws": random.choice(
            [
                "Less than one year",
                "1-2 years",
                "2-5 years",
                "5-10 years",
                "10+ years",
            ]
        ),
        "ZipCode": fake.postcode(),
    }


def _get_product(id=None, company_id=None):
    if id is None:
        id = str(uuid.uuid4())
    if company_id is None:
        company_id = str(uuid.uuid4())

    return {
        "pk": id,
        "sk": "Product",
        "CompanyId": company_id,
        "Title": _fake_title(),
        "Description": fake.paragraph(),
    }


def _get_event(id=None, company_id=None):
    if id is None:
        id=str(uuid.uuid4())
    if company_id is None:
        company_id = str(uuid.uuid4())

    return {
        "pk": id,
        "sk": "Event",
        "CompanyId": company_id,
        "Title": _fake_title(),
        "Description": fake.paragraph(),
        "Date": _random_timestamp_millis(),
    }


# TODO just pass in the user and company and assign the fields/data
def _get_application(id=None, company_id=None):
    if id is None:
        id=str(uuid.uuid4())
    if company_id is None:
        company_id = str(uuid.uuid4())

    return {
        "pk": id,
        "sk": "Application",
        "About": fake.paragraph(),
        "InteractionType": random.choice(["Web", "Mobile", "App", "Other"]),
        "FundingRound": random.choice(
            [
                "Bootstrap",
                "Pre-Seed",
                "Seed",
                "Series A",
                "Series B",
                "Series C",
            ]
        ),
        "AwsServices": random.sample(
            ["Activate", "AppSync", "DynamoDB", "Q", "S3", "SageMaker"],
            random.randint(1, 3),
        ),
        "PrimaryChallenges": random.sample(
            [
                "Building scalable architecture",
                "Choosing right tech solutions",
                "Operational excellence",
                "Finding co-founders",
                "Finding talent",
            ],
            random.randint(1, 3),
        ),
        "City": fake.city(),
        "CompanyId": company_id,
        "CompanyName": fake.company(),
        "CompanyEmail": fake.email(),
        "CompanyType": random.choice(
            [
                "Cooperative",
                "Corporation (C-Corp or S-Corp)",
                "Franchise",
                "Government - Federal",
                "Government - Local",
                "Holding Company",
                "Joint Venture",
                "Limited Liability Company (LLC)",
                "Non-Profit Organization",
                "Partnership",
                "Public Company (Publicly Traded)"
                "Sole Proprietorship",
            ]
        ),
        "CompanyWebsite": fake.domain_name(),
        "HasComplianceOptIn": random.choice([True, False]),
        "HasMarketingOptIn": random.choice([True, False]),
        "ComplianceRequesterType": random.choice(
            [
                "Customer",
                "Investor",
                "Partner",
                "Personal",
                "Shareholder",
                "Supplier",
                "Vendor",
            ]
        ),
        "Country": fake.country(),
        "CreatedDate": _random_timestamp_millis(),
        "DisplayName": fake.user_name(),
        "FirstName": fake.first_name(),
        "CompanyFoundedDate": _random_timestamp_millis(),
        "Industries": random.sample(
            [
                "Automotive",
                "Biotechnology",
                "Education",
                "Entertainment",
                "Hospitality",
                "Information Technology",
                "Manufacturing",
                "Pharmaceutical",
                "Retail",
                "Telecommunications",
            ],
            random.randint(1, 5),
        ),
        "IsBlocklisted": random.choice([True, False]),
        "LastName": fake.last_name(),
        "LastAuthenticatedDate": _random_timestamp_millis(),
        "LastUpdatedDate": _random_timestamp_millis(),
        "ProductLaunchDate": _random_timestamp_millis(),
        "AwsAccountIds": random.sample(
            list(
                set(
                    [
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                        str(random.randint(100000000000, 999999999999)),
                    ]
                )
            ),
            random.randint(1, 5),
        ),
        "NumberOfEmployees": random.choice(
            ["1 - 10", "10-20", "20-50", "50-100", "100-1000", "1000+"]
        ),
        "IsFounder": random.choice([True, False]),
        "PhoneNumber": fake.phone_number(),
        "Role": random.choice(
            [
                "Software Engineer",
                "Project Manager",
                "Marketing Coordinator",
                "Human Resources Manager",
                "Financial Analyst",
                "Graphic Designer",
                "Sales Representative",
                "Accountant",
                "Data Scientist",
                "Operations Manager",
            ]
        ),
        "StateOrProvince": fake.state(),
        "TechnologiesUsed": random.sample(
            [
                "Customer Relationship Management (CRM) software",
                "Enterprise Resource Planning (ERP) system",
                "Cloud computing services (e.g., Amazon Web Services, Microsoft Azure, Google Cloud)",
                "Content Management System (CMS) (e.g., WordPress, Drupal, Joomla)",
                "E-commerce platforms (e.g., Shopify, WooCommerce, Magento)",
                "Project management tools (e.g., Asana, Trello, Jira)",
                "Collaboration and communication tools (e.g., Slack, Microsoft Teams, Zoom)",
                "Business intelligence and analytics tools (e.g., Tableau, Power BI, QlikView)",
                "Cybersecurity solutions (e.g., firewalls, antivirus software, VPNs)",
                "Human Resources Management System (HRMS)",
                "Marketing automation platforms (e.g., HubSpot, Marketo, Pardot)",
                "Social media management tools (e.g., Hootsuite, Sprout Social, Buffer)",
                "Accounting and financial software (e.g., QuickBooks, Xero, FreshBooks)",
                "Inventory management systems",
                "Supply chain management software",
                "Customer support and ticketing systems (e.g., Zendesk, Freshdesk)",
                "Document management systems",
                "Virtual meeting and webinar platforms (e.g., GoToMeeting, WebEx)",
                "Email marketing platforms (e.g., Mailchimp, Constant Contact)",
                "Learning Management Systems (LMS) for employee training",
            ],
            random.randint(1, 5),
        ),
        "YearsOnAws": random.choice(
            [
                "Less than one year",
                "1-2 years",
                "2-5 years",
                "5-10 years",
                "10+ years",
            ]
        ),
        "ZipCode": fake.postcode(),
    }
