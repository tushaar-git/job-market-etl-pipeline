"""
Transform module for job market ETL pipeline
Cleans, validates, and normalizes extracted job data
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms raw job data into database-ready format
    
    Responsibilities:
    - Data quality validation
    - Cleaning and normalization
    - Separating into normalized tables
    """
    
    def __init__(self):
        """Initialize transformer with quality tracking"""
        self.stats = {
            'total_jobs': 0,
            'duplicates_removed': 0,
            'invalid_salaries': 0,
            'missing_companies': 0,
            'missing_locations': 0,
            'missing_categories': 0
        }
        
        # Track unique values for normalization
        self.companies = {}  # {name: id}
        self.locations = {}  # {key: id}
        self.categories = {}  # {tag: id}
        
        logger.info("DataTransformer initialized")
    
    def remove_duplicates(self, jobs: List[Dict]) -> List[Dict]:
        """
        Remove duplicate jobs based on job ID
        
        Args:
            jobs: List of raw job dictionaries
            
        Returns:
            List of unique jobs
        """
        seen_ids = set()
        unique_jobs = []
        
        for job in jobs:
            job_id = job.get('id')
            
            if not job_id:
                logger.warning("Job without ID found, skipping")
                continue
            
            if job_id in seen_ids:
                self.stats['duplicates_removed'] += 1
                logger.debug(f"Duplicate job ID {job_id} removed")
                continue
            
            seen_ids.add(job_id)
            unique_jobs.append(job)
        
        logger.info(f"Removed {self.stats['duplicates_removed']} duplicates. "
                   f"{len(unique_jobs)} unique jobs remain.")
        
        return unique_jobs
    
    def validate_salary(self, job: Dict) -> bool:
        """
        Validate salary data
        
        Rules:
        - salary_min and salary_max must be positive
        - salary_min <= salary_max
        - If equal, it's a single salary (valid)
        
        Args:
            job: Job dictionary
            
        Returns:
            True if valid, False otherwise
        """
        salary_min = job.get('salary_min')
        salary_max = job.get('salary_max')
        
        # Missing salary is OK (nullable in database)
        if salary_min is None and salary_max is None:
            return True
        
        # Both must be present if one is present
        if (salary_min is None) != (salary_max is None):
            logger.warning(f"Job {job.get('id')}: Only one salary value present")
            self.stats['invalid_salaries'] += 1
            return False
        
        # Must be positive
        if salary_min <= 0 or salary_max <= 0:
            logger.warning(f"Job {job.get('id')}: Negative salary")
            self.stats['invalid_salaries'] += 1
            return False
        
        # Min must be <= Max
        if salary_min > salary_max:
            logger.warning(f"Job {job.get('id')}: salary_min > salary_max")
            self.stats['invalid_salaries'] += 1
            return False
        
        return True
    
    def parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse ISO format datetime string
        
        Args:
            date_str: ISO format date string (e.g., '2026-02-11T13:49:18Z')
            
        Returns:
            datetime object or None
        """
        if not date_str:
            return None
        
        try:
            # Remove 'Z' and parse
            if date_str.endswith('Z'):
                date_str = date_str[:-1] + '+00:00'
            return datetime.fromisoformat(date_str)
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
    
    def parse_boolean(self, value: Optional[str]) -> Optional[bool]:
        """
        Convert string boolean to actual boolean
        
        Args:
            value: String '0', '1', 'true', 'false', etc.
            
        Returns:
            Boolean or None
        """
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('1', 'true', 'yes')
        
        return bool(value)
    
    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """
        Clean text fields (trim whitespace, handle None)
        
        Args:
            text: Raw text
            
        Returns:
            Cleaned text or None
        """
        if not text:
            return None
        
        if isinstance(text, str):
            cleaned = text.strip()
            return cleaned if cleaned else None
        
        return str(text).strip()
    
    def extract_location(self, job: Dict) -> Optional[Dict]:
        """
        Extract and normalize location data
        
        Args:
            job: Job dictionary with nested location
            
        Returns:
            Dictionary with location fields or None
        """
        location_data = job.get('location')
        
        if not location_data:
            self.stats['missing_locations'] += 1
            return None
        
        area = location_data.get('area', [])
        
        # Build location dictionary
        location = {
            'country': area[0] if len(area) > 0 else None,
            'state': area[1] if len(area) > 1 else None,
            'county': area[2] if len(area) > 2 else None,
            'city': area[3] if len(area) > 3 else None,
            'display_name': self.clean_text(location_data.get('display_name')),
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude')
        }
        
        return location
    
    def extract_company(self, job: Dict) -> Optional[str]:
        """
        Extract company name
        
        Args:
            job: Job dictionary with nested company
            
        Returns:
            Company name or None
        """
        company_data = job.get('company')
        
        if not company_data:
            self.stats['missing_companies'] += 1
            return None
        
        name = company_data.get('display_name')
        return self.clean_text(name)
    
    def extract_category(self, job: Dict) -> Optional[Dict]:
        """Extract category information"""
        category_data = job.get('category')
        
        if not category_data:
            self.stats['missing_categories'] += 1
            return None
        
        label = self.clean_text(category_data.get('label'))
        tag = self.clean_text(category_data.get('tag'))
        
        # Both label and tag must exist
        if not label or not tag:
            self.stats['missing_categories'] += 1
            return None
        
        return {
            'label': label,
            'tag': tag
        }
    
    def transform(self, jobs: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Main transformation pipeline
        
        Processes raw jobs and separates into normalized tables:
        - companies
        - locations  
        - categories
        - jobs
        
        Args:
            jobs: List of raw job dictionaries from API
            
        Returns:
            Tuple of (companies_list, locations_list, categories_list, jobs_list)
        """
        logger.info(f"Starting transformation of {len(jobs)} jobs")
        self.stats['total_jobs'] = len(jobs)
        
        # Step 1: Remove duplicates
        unique_jobs = self.remove_duplicates(jobs)
        
        # Step 2: Validate and clean each job
        cleaned_jobs = []
        for job in unique_jobs:
            # Validate salary
            if not self.validate_salary(job):
                continue  # Skip jobs with invalid salaries
            
            # Transform job
            transformed = self._transform_job(job)
            if transformed:
                cleaned_jobs.append(transformed)
        
        logger.info(f"Cleaned {len(cleaned_jobs)} jobs (filtered {len(unique_jobs) - len(cleaned_jobs)} invalid)")
        
        # Step 3: Normalize into separate tables
        companies_list = self._build_companies_list()
        locations_list = self._build_locations_list()
        categories_list = self._build_categories_list()
        
        logger.info(f"Normalized into {len(companies_list)} companies, "
                   f"{len(locations_list)} locations, {len(categories_list)} categories")
        
        # Log final stats
        logger.info("Transformation complete:")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        
        return companies_list, locations_list, categories_list, cleaned_jobs
    
    def _transform_job(self, job: Dict) -> Optional[Dict]:
        """
        Transform a single job into database-ready format
        
        Args:
            job: Raw job dictionary
            
        Returns:
            Transformed job dictionary or None if invalid
        """
        try:
            # Extract and normalize company
            company_name = self.extract_company(job)
            company_id = self._get_or_create_company_id(company_name) if company_name else None
            
            # Extract and normalize location
            location_data = self.extract_location(job)
            location_id = self._get_or_create_location_id(location_data) if location_data else None
            
            # Extract and normalize category
            category_data = self.extract_category(job)
            category_id = self._get_or_create_category_id(category_data) if category_data else None
            
            # Parse datetime
            created_at = self.parse_datetime(job.get('created'))
            if not created_at:
                logger.warning(f"Job {job.get('id')}: Missing creation date, skipping")
                return None
            
            # Build cleaned job dictionary
            transformed = {
                'job_id': int(job['id']),  # Convert to int
                'title': self.clean_text(job.get('title')),
                'description': self.clean_text(job.get('description')),
                'salary_min': job.get('salary_min'),
                'salary_max': job.get('salary_max'),
                'salary_is_predicted': self.parse_boolean(job.get('salary_is_predicted')),
                'created_at': created_at,
                'redirect_url': job.get('redirect_url'),
                'adref': job.get('adref'),
                'latitude': job.get('latitude'),
                'longitude': job.get('longitude'),
                'country': job.get('source_country'),
                'company_id': company_id,
                'location_id': location_id,
                'category_id': category_id
            }
            
            # Validate required fields
            if not transformed['title']:
                logger.warning(f"Job {job.get('id')}: Missing title, skipping")
                return None
            
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform job {job.get('id')}: {e}")
            return None
        
    def _get_or_create_company_id(self, name: str) -> int:
        """
        Get existing company ID or create new one
        
        Args:
            name: Company name
            
        Returns:
            Company ID
        """
        if name not in self.companies:
            # Assign next available ID
            self.companies[name] = len(self.companies) + 1
        
        return self.companies[name]
    
    def _get_or_create_location_id(self, location: Dict) -> int:
        """
        Get existing location ID or create new one
        
        Args:
            location: Location dictionary
            
        Returns:
            Location ID
        """
        # Create unique key from location components
        key = (
            location.get('country'),
            location.get('state'),
            location.get('county'),
            location.get('city')
        )
        
        if key not in self.locations:
            # Assign next available ID and store full location data
            location_id = len(self.locations) + 1
            self.locations[key] = {
                'id': location_id,
                'data': location
            }
        
        return self.locations[key]['id']
    
    def _get_or_create_category_id(self, category: Dict) -> int:
        """
        Get existing category ID or create new one
        
        Args:
            category: Category dictionary with 'tag' and 'label'
            
        Returns:
            Category ID
        """
        tag = category.get('tag')
        
        if not tag:
            return None
        
        if tag not in self.categories:
            # Assign next available ID and store category data
            category_id = len(self.categories) + 1
            self.categories[tag] = {
                'id': category_id,
                'data': category
            }
        
        return self.categories[tag]['id']
    
    def _build_companies_list(self) -> List[Dict]:
        """
        Build list of unique companies with IDs
        
        Returns:
            List of company dictionaries
        """
        return [
            {'company_id': company_id, 'name': name}
            for name, company_id in self.companies.items()
        ]
    
    def _build_locations_list(self) -> List[Dict]:
        """
        Build list of unique locations with IDs
        
        Returns:
            List of location dictionaries
        """
        locations = []
        for key, value in self.locations.items():
            location = value['data'].copy()
            location['location_id'] = value['id']
            locations.append(location)
        
        return locations
    
    def _build_categories_list(self) -> List[Dict]:
        """
        Build list of unique categories with IDs
        
        Returns:
            List of category dictionaries
        """
        categories = []
        for tag, value in self.categories.items():
            category = value['data'].copy()
            category['category_id'] = value['id']
            categories.append(category)
        
        return categories
    