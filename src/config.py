import os
from dotenv import load_dotenv

#load environment variables from .env file
load_dotenv('config/.env')

class Config:
	"""Configuration management for the ETL pipeline"""
	
	# Adzuna API credentials
	ADZUNA_APP_ID = os.getenv('APP_ID')
	ADZUNA_APP_KEY = os.getenv('APP_KEY')
	
	# Database credentials
	DB_NAME = os.getenv('DB_NAME')
	DB_USER = os.getenv('DB_USER')
	DB_PASSWORD = os.getenv('DB_PASSWORD')
	DB_HOST = os.getenv('DB_HOST')
	DB_PORT = os.getenv('DB_PORT')
	
	@classmethod
	def validate(cls):
		"""Validate that all required config values are present"""
		required = [
			'ADZUNA_APP_ID', 'ADZUNA_APP_KEY', 
			'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT'
		]
		
		missing = []
		for var in required:
			if not getattr(cls, var):
				missing.append(var)

		if missing:
			raise ValueError(f"Missing required config: {', '.join(missing)}")

		return True