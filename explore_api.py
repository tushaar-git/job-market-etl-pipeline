import requests
import json

# Credentials
APP_ID = "371ed058"
APP_KEY = "faee1046fb2a29bfca7525570eb314f7"

url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
params = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "results_per_page": 5,
    "what": "data engineer"
}

response = requests.get(url, params=params)
print(f"Status Code: {response.status_code}")

data = response.json()

# Save to file (NoneType)
with open("sample_response.json", "w") as f:
    json.dump(data, f, indent=2)

# Print to console (String)
print(json.dumps(data, indent=2))
