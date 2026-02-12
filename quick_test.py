from src.config import Config

print(f"Config.ADZUNA_APP_ID: {Config.ADZUNA_APP_ID}")
print(f"Config.ADZUNA_APP_KEY: {Config.ADZUNA_APP_KEY}")

from src.extract import AdzunaClient

client = AdzunaClient(
    app_id=Config.ADZUNA_APP_ID,
    app_key=Config.ADZUNA_APP_KEY
)

print(f"\nClient app_id: {client.app_id}")
print(f"Client app_key: {client.app_key}")