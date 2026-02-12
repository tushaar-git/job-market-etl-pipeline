import requests
import os
from dotenv import load_dotenv

# Load .env
load_dotenv('config/.env')

app_id = os.getenv('APP_ID')
app_key = os.getenv('APP_KEY')

print(f"APP_ID from .env: {app_id}")
print(f"APP_KEY from .env: {app_key}")
print()

# Make direct API call
url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
params = {
    "app_id": app_id,
    "app_key": app_key,
    "results_per_page": 1,
    "what": "engineer"
}

print("Making direct API request...")
response = requests.get(url, params=params)

print(f"Status Code: {response.status_code}")
print(f"Response: {response.text[:500]}")  # First 500 chars

if response.status_code == 200:
    print("\n✅ API call successful!")
    data = response.json()
    print(f"Total jobs: {data.get('count')}")
elif response.status_code == 401:
    print("\n❌ Authentication failed - check your credentials")
    print("Go to: https://developer.adzuna.com/ and verify your keys")