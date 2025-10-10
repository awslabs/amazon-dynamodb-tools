import csv
import random
import uuid

from faker import Faker

fake = Faker()
Faker.seed(42)  # For reproducibility

OUTPUT_FILE = "fake_people.csv"
NUM_ROWS = 1_000_000

# Define headers
headers = [
    "id", "first_name", "last_name", "email", "age", "phone",
    "city", "country", "occupation", "hobby"
]

# Sample occupations and hobbies for realism
occupations = ["Engineer", "Teacher", "Architect", "Doctor", "Nurse", "Artist", "Chef", "Lawyer"]
hobbies = ["Photography", "Golf", "Reading", "Cooking", "Traveling", "Painting", "Running", "Gaming"]

with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(headers)

    for _ in range(NUM_ROWS):
        person_id = str(uuid.uuid4())
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        age = random.randint(18, 80)
        phone = fake.phone_number() if random.random() > 0.1 else ""
        city = fake.city() if random.random() > 0.2 else ""
        country = fake.country() if random.random() > 0.2 else ""
        occupation = random.choice(occupations) if random.random() > 0.1 else ""
        hobby = random.choice(hobbies) if random.random() > 0.2 else ""

        writer.writerow([
            person_id, first_name, last_name, email, age, phone,
            city, country, occupation, hobby
        ])
