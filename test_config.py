from src.config import Config

# Validate configuration
try:
	Config.validate()
	print("Configuration loaded successfully")
	print(f"API ID: {Config.ADZUNA_APP_ID[:10]}...")
	print(f"Database: {Config.DB_NAME}")
except ValueError as e:
	print(f"Configuration error: {e}")