import csv
import random
import string

def generate_dirty_value(value, dtype):
    """Generate dirty data based on the type of the value."""
    if random.random() < 0.001:  # 0.1% probability
        if dtype == 'int':
            options = [None, -abs(random.randint(1, 100)), ''.join(random.choices(string.ascii_letters, k=5))]
            return random.choice(options)
        elif dtype == 'object':
            options = [None, ''.join(random.choices(string.punctuation + string.digits, k=5))]
            return random.choice(options)
    return value

def generate_products_csv(filename, num_rows):
    categories = ['Beverages', 'Snacks', 'Dairy', 'Bakery', 'Frozen', 'Produce', 'Meat', 'Seafood']
    product_names = [
        "Rice", "Pasta", "Bread", "Milk", "Eggs", "Butter", "Cheese", "Yogurt", "Flour", "Sugar", 
        "Salt", "Pepper", "Olive Oil", "Tomato Sauce", "Cereal", "Coffee", "Tea", "Honey", "Jam", "Peanut Butter",
        "Chicken", "Beef", "Fish", "Pork", "Carrots", "Potatoes", "Onions", "Apples", "Bananas", "Oranges",
        "Lettuce", "Spinach", "Cucumbers", "Tomatoes", "Garlic", "Ginger", "Baking Powder", "Soda", "Vinegar", "Spices",
        "Chips", "Crackers", "Cookies", "Chocolate", "Juice", "Water", "Soda", "Ice Cream", "Nuts", "Dried Fruit"
    ]

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['product_id', 'product_name', 'category', 'price', 'stock_quantity'])

        for product_id in range(1, num_rows + 1):
            product_name = random.choice(product_names)
            category = random.choice(categories)
            price = random.randint(1, 100)
            stock_quantity = random.randint(1, 50)

            # Introduce dirty data
            product_name = generate_dirty_value(product_name, 'object')
            category = generate_dirty_value(category, 'object')
            price = generate_dirty_value(price, 'int')
            stock_quantity = generate_dirty_value(stock_quantity, 'int')

            writer.writerow([product_id, product_name, category, price, stock_quantity])
        
        print("done")

# Generate the CSV file
generate_products_csv('Products/products.csv', 50)
