"""
Complete ETL Pipeline for Job Market Data
Extracts from Adzuna API -> Transforms -> Loads to PostgreSQL
"""
import logging
import sys
from datetime import datetime
from src.config import Config
from src.extract import AdzunaClient
from src.transform import DataTransformer
from src.load import DatabaseLoader

# Setup logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Run complete ETL pipeline"""
    
    try:
        logger.info("=" * 70)
        logger.info("JOB MARKET ETL PIPELINE - STARTED")
        logger.info("=" * 70)
        
        # Validate configuration
        logger.info("Step 1: Validating configuration...")
        Config.validate()
        logger.info("✓ Configuration valid")
        
        # ==================== EXTRACT ====================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1: EXTRACT")
        logger.info("=" * 70)
        
        client = AdzunaClient(
            app_id=Config.ADZUNA_APP_ID,
            app_key=Config.ADZUNA_APP_KEY,
            country="us"
        )
        
        # Get total available jobs
        query = "data engineer"
        total_jobs = client.get_job_count(query)
        logger.info(f"Total jobs available for '{query}': {total_jobs:,}")
        
        # Extract jobs
        logger.info("Extracting jobs from API...")
        jobs = client.search_jobs(
            query=query,
            max_pages=5,
            results_per_page=50
        )
        
        if not jobs:
            logger.error("No jobs extracted. Aborting pipeline.")
            sys.exit(1)
        
        logger.info(f"✓ Extracted {len(jobs)} jobs")
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_jobs_{timestamp}.json"
        client.save_raw_data(jobs, filename)
        logger.info(f"✓ Saved raw data to data/{filename}")
        
        # ==================== TRANSFORM ====================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2: TRANSFORM")
        logger.info("=" * 70)
        
        transformer = DataTransformer()
        companies, locations, categories, transformed_jobs = transformer.transform(jobs)
        
        logger.info("✓ Transformation complete:")
        logger.info(f"  - Companies: {len(companies)}")
        logger.info(f"  - Locations: {len(locations)}")
        logger.info(f"  - Categories: {len(categories)}")
        logger.info(f"  - Jobs: {len(transformed_jobs)}")
        
        # ==================== LOAD ====================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3: LOAD")
        logger.info("=" * 70)
        
        loader = DatabaseLoader()
        summary = loader.load_all(companies, locations, categories, transformed_jobs)
        loader.close()
        
        logger.info("OK- Load complete")
        
        # ==================== SUMMARY ====================
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE COMPLETE - SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Query: '{query}'")
        logger.info(f"Total available: {total_jobs:,}")
        logger.info(f"Extracted: {len(jobs)}")
        logger.info(f"Transformed: {len(transformed_jobs)}")
        logger.info(f"Loaded to database:")
        logger.info(f"  - Companies: {summary['companies']}")
        logger.info(f"  - Locations: {summary['locations']}")
        logger.info(f"  - Categories: {summary['categories']}")
        logger.info(f"  - Jobs: {summary['jobs']}")
        logger.info(f"API requests: {client.request_count}")
        logger.info(f"Raw data: data/{filename}")
        logger.info("=" * 70)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())