import csv
import random
import string
import datetime
import os

def generate_orders_csv(filename, target_size):
    # Define the column headers
    headers = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "total_amount",
        "transaction_date",
        "payment_method"
    ]
    
    # Define realistic value ranges
    customer_ids = list(range(1, 51))
    product_ids = list(range(1, 151))
    quantities = list(range(1, 21))
    total_amounts = list(range(50, 50001))
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Cash", None]
    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Calculate the number of rows to generate based on target file size
    sample_row = [1000000000, 10, 10, 10, 500, "2019-01-01", "Cash"]
    sample_size = sum(len(str(x)) for x in sample_row) + len(sample_row) - 1
    estimated_rows = target_size * 1024 * 1024 // sample_size
    
    def random_date(start, end):
        """Generate a random date between start and end."""
        return start + datetime.timedelta(days=random.randint(0, (end - start).days))

    def introduce_dirty_data(value, column):
        """Introduce dirty data into a given value with a 10% probability."""
        if random.random() > 0.1:
            return value
        if column in ["quantity", "total_amount"]:
            dirty_options = [None, "NaN", -random.randint(1, 20)]
            return random.choice(dirty_options)
        elif column == "payment_method":
            return None
        return value
    
    # Write the CSV file
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        order_id = 1
        current_size = 0
        print("Generating orders.csv...")
        
        while current_size < target_size * 1024 * 1024:
            customer_id = random.choice(customer_ids)
            product_id = random.choice(product_ids)
            quantity = introduce_dirty_data(random.choice(quantities), "quantity")
            total_amount = introduce_dirty_data(random.choice(total_amounts), "total_amount")
            transaction_date = random_date(start_date, end_date).isoformat()
            payment_method = introduce_dirty_data(random.choice(payment_methods), "payment_method")
            
            row = [order_id, customer_id, product_id, quantity, total_amount, transaction_date, payment_method]
            writer.writerow(row)
            
            # Track progress
            order_id += 1
            current_size = file.tell()
            if order_id % 100000 == 0:
                print(f"Generated {order_id} rows... Current file size: {current_size / (1024 * 1024):.2f} MB")
        
        print(f"File generation complete. Total rows: {order_id - 1}")

# Specify the filename and target size in GB
filename = "Order/orders.csv"
target_size = 210  # in GB

generate_orders_csv(filename, target_size)
