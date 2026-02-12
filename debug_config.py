from src.config import Config
import os

print("=== Debugging Configuration ===\n")

# Check if .env file exists
env_path = 'config/.env'
if os.path.exists(env_path):
    print(f"✅ .env file exists at: {env_path}")
else:
    print(f"❌ .env file NOT found at: {env_path}")

print("\n=== Environment Variables ===")
print(f"APP_ID: {Config.ADZUNA_APP_ID}")
print(f"APP_KEY: {Config.ADZUNA_APP_KEY}")
print(f"APP_ID is None: {Config.ADZUNA_APP_ID is None}")
print(f"APP_KEY is None: {Config.ADZUNA_APP_KEY is None}")

if Config.ADZUNA_APP_ID:
    print(f"\nAPP_ID length: {len(Config.ADZUNA_APP_ID)} characters")
    print(f"APP_ID first 5 chars: {Config.ADZUNA_APP_ID[:5]}...")
    
if Config.ADZUNA_APP_KEY:
    print(f"APP_KEY length: {len(Config.ADZUNA_APP_KEY)} characters")
    print(f"APP_KEY first 5 chars: {Config.ADZUNA_APP_KEY[:5]}...")

# Test validation
print("\n=== Validation Test ===")
try:
    Config.validate()
    print("✅ Validation passed")
except ValueError as e:
    print(f"❌ Validation failed: {e}")