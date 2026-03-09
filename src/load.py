"""
Load module for job market ETL pipeline
Handles database connections and data insertion
"""

import psycopg2
from psycopg2 import sql
import logging
from typing import List, Dict, Optional
from src.config import Config

logger = logging.getLogger(__name__)

class DatabaseLoader:
    """
    Loads transformed data into PostgreSQL database

    Handles:
    - Database connection management
    - Inserting normalized data (companies, locations, categories, jobs)
    - Transaction management
    - Error handling
    """
    def __init__(self):
        """Initialize database connection"""
        self.connection = None
        self.cursor = None
        self._connect()
        logger.info("DatabseLoader initialized")
    
    def _connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                dbname=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                host=Config.DB_HOST,
                port=Config.DB_PORT
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to databse: {Config.DB_NAME}")
        
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to databse: {2}")
            raise
    
    def close(self):
        """Close databse connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def load_companies(self, companies: List[Dict]) -> Dict[int, int]:
            """
            Load companies into database and return ID mapping
            
            Args:
                companies: List of company dictionaries with company_id and name
                
            Returns:
                Dictionary mapping transform company_id -> database company_id
            """
            if not companies:
                logger.warning("No companies to load")
                return {}
            
            try:
                id_mapping = {}
                
                for company in companies:
                    name = company.get('name')
                    transform_id = company.get('company_id')
                    
                    if not name:
                        continue
                    
                    # Insert or get existing
                    self.cursor.execute("""
                        INSERT INTO companies (name)
                        VALUES (%s)
                        ON CONFLICT (name) DO NOTHING
                        RETURNING company_id
                    """, (name,))
                    
                    result = self.cursor.fetchone()
                    
                    if result:
                        # New insert
                        db_id = result[0]
                    else:
                        # Already exists, fetch it
                        self.cursor.execute(
                            "SELECT company_id FROM companies WHERE name = %s",
                            (name,)
                        )
                        db_id = self.cursor.fetchone()[0]
                    
                    # Map transform ID -> database ID
                    id_mapping[transform_id] = db_id
                
                logger.info(f"Loaded {len(id_mapping)} companies")
                return id_mapping
                
            except psycopg2.Error as e:
                logger.error(f"Failed to load companies: {e}")
                self.connection.rollback()
                raise

    def load_locations(self, locations: List[Dict]) -> Dict[int, int]:
            """
            Load locations into database and return ID mapping
            
            Args:
                locations: List of location dictionaries
                
            Returns:
                Dictionary mapping transform location_id -> database location_id
            """
            if not locations:
                logger.warning("No locations to load")
                return {}
            
            try:
                id_mapping = {}
                
                for location in locations:
                    transform_id = location.get('location_id')
                    
                    # Insert or get existing
                    self.cursor.execute("""
                        INSERT INTO locations (country, state, county, city, display_name, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country, state, county, city) DO NOTHING
                        RETURNING location_id
                    """, (
                        location.get('country'),
                        location.get('state'),
                        location.get('county'),
                        location.get('city'),
                        location.get('display_name'),
                        location.get('latitude'),
                        location.get('longitude')
                    ))
                    
                    result = self.cursor.fetchone()
                    
                    if result:
                        db_id = result[0]
                    else:
                        # Already exists, fetch it
                        self.cursor.execute("""
                            SELECT location_id FROM locations 
                            WHERE country IS NOT DISTINCT FROM %s
                            AND state IS NOT DISTINCT FROM %s
                            AND county IS NOT DISTINCT FROM %s
                            AND city IS NOT DISTINCT FROM %s
                        """, (
                            location.get('country'),
                            location.get('state'),
                            location.get('county'),
                            location.get('city')
                        ))
                        db_id = self.cursor.fetchone()[0]
                    
                    id_mapping[transform_id] = db_id
                
                logger.info(f"Loaded {len(id_mapping)} locations")
                return id_mapping
                
            except psycopg2.Error as e:
                logger.error(f"Failed to load locations: {e}")
                self.connection.rollback()
                raise

    def load_categories(self, categories: List[Dict]) -> Dict[int, int]:
            """
            Load categories into database and return ID mapping
            
            Args:
                categories: List of category dictionaries
                
            Returns:
                Dictionary mapping transform category_id -> database category_id
            """
            if not categories:
                logger.warning("No categories to load")
                return {}
            
            try:
                id_mapping = {}
                
                for category in categories:
                    label = category.get('label')
                    tag = category.get('tag')
                    transform_id = category.get('category_id')
                    
                    if not label or not tag:
                        continue
                    
                    # Insert or get existing
                    self.cursor.execute("""
                        INSERT INTO categories (label, tag)
                        VALUES (%s, %s)
                        ON CONFLICT (tag) DO NOTHING
                        RETURNING category_id
                    """, (label, tag))
                    
                    result = self.cursor.fetchone()
                    
                    if result:
                        db_id = result[0]
                    else:
                        # Already exists
                        self.cursor.execute(
                            "SELECT category_id FROM categories WHERE tag = %s",
                            (tag,)
                        )
                        db_id = self.cursor.fetchone()[0]
                    
                    id_mapping[transform_id] = db_id
                
                logger.info(f"Loaded {len(id_mapping)} categories")
                return id_mapping
                
            except psycopg2.Error as e:
                logger.error(f"Failed to load categories: {e}")
                self.connection.rollback()
                raise

    def load_jobs(self, jobs: List[Dict], company_mapping: Dict, 
                    location_mapping: Dict, category_mapping: Dict) -> int:
            """
            Load jobs into database using ID mappings
            
            Args:
                jobs: List of job dictionaries
                company_mapping: Transform ID -> DB ID mapping for companies
                location_mapping: Transform ID -> DB ID mapping for locations
                category_mapping: Transform ID -> DB ID mapping for categories
                
            Returns:
                Number of jobs inserted
            """
            if not jobs:
                logger.warning("No jobs to load")
                return 0
            
            try:
                insert_query = """
                    INSERT INTO jobs (
                        job_id, title, description, salary_min, salary_max,
                        salary_is_predicted, created_at, redirect_url, adref,
                        latitude, longitude, company_id, location_id, category_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO NOTHING
                """
                
                job_data = []
                for job in jobs:
                    # Map transform IDs to database IDs
                    transform_company_id = job.get('company_id')
                    transform_location_id = job.get('location_id')
                    transform_category_id = job.get('category_id')
                    
                    db_company_id = company_mapping.get(transform_company_id) if transform_company_id else None
                    db_location_id = location_mapping.get(transform_location_id) if transform_location_id else None
                    db_category_id = category_mapping.get(transform_category_id) if transform_category_id else None
                    
                    job_data.append((
                        job.get('job_id'),
                        job.get('title'),
                        job.get('description'),
                        job.get('salary_min'),
                        job.get('salary_max'),
                        job.get('salary_is_predicted'),
                        job.get('created_at'),
                        job.get('redirect_url'),
                        job.get('adref'),
                        job.get('latitude'),
                        job.get('longitude'),
                        db_company_id,  # Use database ID!
                        db_location_id,  # Use database ID!
                        db_category_id   # Use database ID!
                    ))
                
                self.cursor.executemany(insert_query, job_data)
                
                rows_inserted = self.cursor.rowcount
                logger.info(f"Loaded {rows_inserted} jobs")
                
                return rows_inserted
                
            except psycopg2.Error as e:
                logger.error(f"Failed to load jobs: {e}")
                self.connection.rollback()
                raise

    def load_all(self, companies: List[Dict], locations: List[Dict], 
                    categories: List[Dict], jobs: List[Dict]) -> Dict:
            """
            Load all data in correct order with ID mapping
            """
            try:
                logger.info("=" * 60)
                logger.info("Starting database load")
                logger.info("=" * 60)
                
                # Load reference tables and get ID mappings
                company_mapping = self.load_companies(companies)
                location_mapping = self.load_locations(locations)
                category_mapping = self.load_categories(categories)
                
                # Load jobs with ID mappings
                jobs_loaded = self.load_jobs(jobs, company_mapping, location_mapping, category_mapping)
                
                # Commit transaction
                self.connection.commit()
                logger.info("All data committed to database")
                
                summary = {
                    'companies': len(company_mapping),
                    'locations': len(location_mapping),
                    'categories': len(category_mapping),
                    'jobs': jobs_loaded
                }
                
                logger.info("=" * 60)
                logger.info("Load Summary:")
                for table, count in summary.items():
                    logger.info(f"  {table}: {count} rows")
                logger.info("=" * 60)
                
                return summary
                
            except Exception as e:
                logger.error(f"Load failed, rolling back: {e}")
                self.connection.rollback()
                raise