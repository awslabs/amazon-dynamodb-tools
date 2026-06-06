import random
import string
import uuid
from decimal import Decimal


FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna",
    "Kenneth", "Michelle", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth",
    "Jose", "Julie", "Nathan", "Olivia", "Henry", "Joyce", "Peter", "Virginia",
    "Adam", "Victoria", "Douglas", "Kelly", "Zachary", "Lauren", "Walter", "Christina",
    "Harold", "Joan", "Kyle", "Evelyn", "Carl", "Judith", "Arthur", "Megan",
    "Dylan", "Andrea", "Gabriel", "Cheryl", "Roger", "Hannah", "Bruce", "Jacqueline",
    "Albert", "Martha", "Wayne", "Gloria", "Eugene", "Teresa", "Russell", "Ann",
    "Philip", "Sara", "Randy", "Madison", "Harry", "Frances", "Vincent", "Kathryn",
    "Bobby", "Janice", "Johnny", "Jean", "Howard", "Abigail", "Roy", "Alice",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales",
    "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
    "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward",
    "Richardson", "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray",
    "Mendoza", "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel",
    "Myers", "Long", "Ross", "Foster", "Jimenez", "Powell",
]

COUNTRIES = [
    "US", "GB", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX",
    "ES", "IT", "NL", "SE", "NO", "DK", "FI", "PL", "KR", "SG",
    "NZ", "IE", "CH", "AT", "BE", "PT", "AR", "CL", "CO", "ZA",
]

STATUSES = ["active", "inactive", "suspended"]
STATUS_WEIGHTS = [0.80, 0.15, 0.05]

PLANS = ["free", "pro", "enterprise"]
PLAN_WEIGHTS = [0.60, 0.30, 0.10]

DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "protonmail.com", "fastmail.com", "zoho.com", "aol.com", "mail.com",
]


def _random_iso_date(start_year=2018, end_year=2026):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"


def generate():
    user_id = str(uuid.uuid4())
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"

    username = f"{first.lower()}{last.lower()}{random.randint(1, 9999)}"
    domain = random.choice(DOMAINS)
    email = f"{username}@{domain}"

    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    plan = random.choices(PLANS, weights=PLAN_WEIGHTS, k=1)[0]
    country = random.choice(COUNTRIES)
    age = Decimal(random.randint(18, 85))
    login_count = Decimal(random.randint(0, 5000))

    created_at = _random_iso_date(2018, 2024)
    last_login = _random_iso_date(2024, 2026)

    item = {
        'user_id': user_id,
        'email': email,
        'name': name,
        'status': status,
        'plan': plan,
        'created_at': created_at,
        'last_login': last_login,
        'age': age,
        'country': country,
        'login_count': login_count,
    }

    return item
