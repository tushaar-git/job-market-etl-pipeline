from src.load import DatabaseLoader
from src.transform import DataTransformer
from src.config import Config
import json

# Load raw data
with open('data/raw_jobs_20260308_080749.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

jobs = data['jobs']

print("=" * 60)
print("TESTING LOAD MODULE")
print("=" * 60)

# Step 1: Transform data
print("\n1. Transforming data...")
transformer = DataTransformer()
companies, locations, categories, transformed_jobs = transformer.transform(jobs)

print(f"   Companies: {len(companies)}")
print(f"   Locations: {len(locations)}")
print(f"   Categories: {len(categories)}")
print(f"   Jobs: {len(transformed_jobs)}")

# Step 2: Load to database
print("\n2. Loading to database...")
Config.validate()

loader = DatabaseLoader()
summary = loader.load_all(companies, locations, categories, transformed_jobs)

print("\n3. Load complete!")
print(f"   Summary: {summary}")

loader.close()

print("\n" + "=" * 60)
print("TEST COMPLETE - Check pgAdmin to see your data!")
print("=" * 60)