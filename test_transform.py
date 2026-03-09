from src.transform import DataTransformer
import json

# Load raw data
with open('data/raw_jobs_20260308_080749.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

jobs = data['jobs']

print("=" * 60)
print("TRANSFORM MODULE TEST")
print("=" * 60)

# Initialize transformer
transformer = DataTransformer()

# Run full transformation
companies, locations, categories, transformed_jobs = transformer.transform(jobs)

print("\n=== Transformation Results ===")
print(f"Input: {len(jobs)} raw jobs")
print(f"Output: {len(transformed_jobs)} transformed jobs")
print(f"Companies: {len(companies)} unique")
print(f"Locations: {len(locations)} unique")
print(f"Categories: {len(categories)} unique")

print("\n=== Sample Company ===")
print(json.dumps(companies[0], indent=2))

print("\n=== Sample Location ===")
print(json.dumps(locations[0], indent=2, default=str))

print("\n=== Sample Category ===")
print(json.dumps(categories[0], indent=2))

print("\n=== Sample Transformed Job ===")
print(json.dumps(transformed_jobs[0], indent=2, default=str))

print("\n=== Data Quality Stats ===")
print(json.dumps(transformer.stats, indent=2))

# Verify foreign key relationships
print("\n=== Foreign Key Validation ===")
job_sample = transformed_jobs[0]
print(f"Job ID: {job_sample['job_id']}")
print(f"  → Company ID: {job_sample['company_id']}")
print(f"  → Location ID: {job_sample['location_id']}")
print(f"  → Category ID: {job_sample['category_id']}")

# Find matching company
matching_company = next(c for c in companies if c['company_id'] == job_sample['company_id'])
print(f"  → Company Name: {matching_company['name']}")

# Find matching location
matching_location = next(l for l in locations if l['location_id'] == job_sample['location_id'])
print(f"  → Location: {matching_location['display_name']}")

print("\n" + "=" * 60)
print("TRANSFORMATION COMPLETE")
print("=" * 60)