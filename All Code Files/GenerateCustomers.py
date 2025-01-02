import csv
import random
import string
from tqdm import tqdm

# Define constants
NUM_CUSTOMERS = 150
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
    "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco",
    "Indianapolis", "Seattle", "Denver", "Washington"
]

# Helper functions to generate data
def random_name():
    if random.random() < 0.1:  # 10% chance of dirty data
        return "" if random.random() < 0.5 else ''.join(random.choices(string.punctuation, k=5))
    return random.choice(["John", "Jane", "Alex", "Emily", "Chris", "Katie", "Mike", "Sarah", "David", "Laura"]) + \
           " " + random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Martinez", "Davis", "Hernandez", "Lopez"])

def random_email(name):
    if random.random() < 0.1:  # 10% chance of dirty data
        return "" if random.random() < 0.5 else "invalid_email"
    username = name.split()[0].lower() if name else "unknown"
    domain = random.choice(["hotmail.com", "icloud.com", "gmail.com", "yahoo.com"])
    return f"{username}@{domain}"

def random_phone():
    if random.random() < 0.1:  # 10% chance of dirty data
        return "" if random.random() < 0.5 else None
    return float(random.randint(1000000000, 9999999999))

def random_age():
    if random.random() < 0.1:  # 10% chance of dirty data
        return "" if random.random() < 0.5 else random.choice(["NaN", -1 * random.randint(1, 100)])
    return random.randint(18, 90)

def random_gender():
    if random.random() < 0.1:  # 10% chance of dirty data
        return None
    return random.choice(["M", "F", "O"])

def random_location():
    if random.random() < 0.01:  # 10% chance of dirty data
        return ""
    return random.choice(CITIES)

# Generate and write the CSV file
def generate_customers_csv(filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "name", "email", "phone", "age", "gender", "location"])

        for customer_id in tqdm(range(1, NUM_CUSTOMERS + 1), desc="Generating customers"):
            name = random_name()
            email = random_email(name)
            phone = random_phone()
            age = random_age()
            gender = random_gender()
            location = random_location()

            writer.writerow([customer_id, name, email, phone, age, gender, location])

# Generate the file
generate_customers_csv("Customer/customers.csv")
