import pandas as pd
import random

def generate_catalog():
    # Categories from our producer
    categories = ['Electronics', 'Fashion', 'Home', 'Books']
    
    data = []
    # Generate 100 fake products
    for i in range(1, 101):
        cat = random.choice(categories)
        price = round(random.uniform(20, 500), 2)
        # Cost is 60-80% of price
        cost = round(price * random.uniform(0.6, 0.8), 2)
        
        data.append({
            "product_id": f"PROD-{i}",
            "category": cat,
            "supplier_price": cost,
            "supplier_name": f"Supplier_{random.randint(1, 10)}"
        })
        
    df = pd.DataFrame(data)
    # Save to reference data folder
    df.to_csv("data/product_catalog.csv", index=False)
    print(" Product Catalog Generated: data/product_catalog.csv")

if __name__ == "__main__":
    generate_catalog()