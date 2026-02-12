"""
Main extraction script for the job market ETL pipeline
Extracts job data from Adzuna API and saved to JSON
"""

from src.extract import AdzunaClient
from src.config import Config
from datetime import datetime
from src.logging_config import setup_logging
import logging

# Set up logging
setup_logging(log_level=logging.INFO, log_to_file=True)

logger = logging.getLogger(__name__)

def main():
	"""
	Main extraction workflow
	"""
	try:
		# Validte configuration
		logger.info("=" * 60)
		logger.info("Starting job Market Data Extraction")
		logger.info("=" * 60)
	
		Config.validate()
		logger.info("✓ Configuration validated")

		# Initialize client
		client = AdzunaClient(
			app_id=Config.ADZUNA_APP_ID,
			app_key=Config.ADZUNA_APP_KEY,
			country="us"
		)
		logger.info("✓ API client initialized")

		# Get job count first
		query = "data engineer"
		total_available = client.get_job_count(query)
		logger.info(f"Total jobs available for '{query}': {total_available:,}")

		# Extract jobs (5 pages * 50 per page = 250 pages)
		logger.info("Starting extraction...")
		jobs = client.search_jobs(
			query=query,
			max_pages=5,
			results_per_page=50
		)
	
		if not jobs:
			logger.error("No jobs extracted. Aborting.")
			return

		logger.info(f"✓ Extracted {len(jobs)} jobs")

		# Save raw data
		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		filename = f"raw_jobs_{timestamp}.json"
		client.save_raw_data(jobs, filename)

		# Summary
		logger.info("="  * 60)
		logger.info("Extraction Complete!")
		logger.info(f" Jobs extracted: {len(jobs)}")
		logger.info(f" API requests made: {client.request_count}")
		logger.info(f" Data saved to: data/{filename}")
		logger.info("=" * 60)

	except Exception as e:
		logger.error(f"Extraction failed: {e}", exc_info=True)
		raise

if __name__ == "__main__":
	main()