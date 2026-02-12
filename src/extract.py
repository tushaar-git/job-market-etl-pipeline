import requests
import time
import logging
from typing import Dict, List, Optional
from src.config import Config # Config class

# Set up logging
logging.basicConfig(
	level=logging.INFO,
    	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__) # Create logger for this module

class AdzunaClient:
	"""
	Client for interacting with the Adzuna Jobs API
	
	Handles authentication, pagination, error handling, and rate limiting.
	"""

	def __init__(self, app_id: str, app_key: str, country: str = "us"):
		"""
		Initialize the Adzuna API client

		Args:
			app_id: Adzuna application ID
			app_key: Adzuna application key
			country: Country code (default: 'us')
		"""
		self.app_id = app_id
		self.app_key = app_key
		self.country = country
		self.base_url = f"https://api.adzuna.com/v1/api/jobs/{country}/search"
		self.request_count = 0 # Monitoring API usage
		self.session = requests.Session() # Reuse connections for efficiency instead of creating new ones for each requests

		logger.info(f"Initialized AdzunaClient for country: {country}")

	def _build_params(self, query: str, page: int, results_per_page: int = 20) -> Dict:
		"""
		Build query parameters for API request
		
		Args:
			query: Search query (e.g., 'data engineer')
			page: Page number (starts at 1)
			results_per_page: Number of results per page (max 50)
		
		Returns:
			Dictionary of query parameters
		"""
		return {
			"app_id": self.app_id,
			"app_key": self.app_key,
			"what": query,
			"results_per_page": min(results_per_page, 50), # API limit cap at 50
		}
	def _make_request(self, url:str, params: Dict, max_retries: int = 3) -> Optional[Dict]:
		"""
		Make API requests with retry logic and error handling
		
		Args:
			url: Full URL to request
			params: Query parameters
			max_retries: Maximum number of retry attempts
		
		Returns:
			JSON response as dictionary, or None if all retries fail
		"""
		for attempt in range(max_retries):
			try:
				logger.info(f"Making request to {url} (attempt {attempt + 1}/{max_retries})")
				
				logger.debug(f"URL: {url}")
				logger.debug(f"Params: {params}")			

				response = self.session.get(url, params = params, timeout = 10)
				self.request_count += 1

				logger.debug(f"Response status: {response.status_code}")
				logger.debug(f"Response URL: {response.url}")
				
				# Get HTTP status
				if response.status_code == 200:
					logger.info(f"Request successful (total requests: {self.request_count})")
					return response.json()
				elif response.status_code == 429:
					# Rate limit hit
					logger.warning("Rate limit hit. Waiting 60 seconds...")
					time.sleep(60)
					continue
				elif response.status_code == 401:
					logger.error("Authentication failed. Check your API credentials.")
					return None
				else:
					logger.warning(f"Request failed with status {response.status_code}")
					logger.warning(f"Response: {response.text[:200]}")
		
			except requests.exceptions.Timeout: # Network too slow
				logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
			except requests.exceptions.ConnectionError: # Can't reach server
				logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries})")
			except requests.exceptions.RequestsException as e: # Any other request error
				logger.error(f"Request failed: {e}")
				return None
		
			# Wait before retry (exponential backoff)
			if attempt < max_retries - 1:
				wait_time = 2 ** attempt # 1s, 2s, 4s
				logger.info(f"Waiting {wait_time}s before retry...")
				time.sleep(wait_time)

		logger.error(f"All {max_retries} attempts failed")
		return None
	
	def search_jobs(self, query: str, max_pages: int = 5, results_per_page: int = 20) -> List[Dict]:
		"""
		Search for jobs with pagination support
		
		Args:
			query: Search term (e.g., 'data engineer')
			max_pages: Maximum number of pages to fetch
			results_per_page: Results per page (max 50)
		Returns:
			List of job dictionaries
		"""
		all_jobs = []
		
		logger.info(f"Starting job search: query='{query}', max_pages={max_pages}")
		
		for page in range(1, max_pages + 1): # Automatically fetch multiple pages
			url = f"{self.base_url}/{page}"
			params = self._build_params(query, page, results_per_page)
			
			logger.info(f"Fetching page {page}/{max_pages}")
			
			response_data = self._make_request(url, params)
		
			if response_data is None:
				logger.warning(f"Failed to fetch page {page}. Stopping pagination.")
				break
			
			# Extract jobs from response
			jobs = response_data.get('results', [])
		
			if not jobs:
				logger.info(f"No more jobs found at page {page}. Stopping.")
				break # No more results, stop early
	
			all_jobs.extend(jobs)
			logger.info(f"Collected {len(jobs)} jobs from page {page} (total: {len(all_jobs)})")
			
			# Respect rate limits (small delay between pages)
			if page < max_pages:
				time.sleep(1)
		logger.info(f"Search complete. Total jobs collected: {len(all_jobs)}")
		return all_jobs
	
	def get_job_count(self, query: str) -> int:
		"""
		Get total number of jobs matching quey (without fetching all)
		
		Args:
			query: Search term

		Returns:
			Toal job count
		"""
		url = f"{self.base_url}/1"
		params = self._build_params(query, 1, 1) # Just fetch 1 result
	
		response_data = self._make_request(url, params)
	
		if response_data:
			count = response_data.get('count', 0)
			logger.info(f"Found {count} total jobs for query '{query}'")
			return count
		return 0