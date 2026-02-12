from src.extract import AdzunaClient
from src.config import Config

def main():
    # Validate config
    Config.validate()
    
    # Initialize client
    client = AdzunaClient(
        app_id=Config.ADZUNA_APP_ID,
        app_key=Config.ADZUNA_APP_KEY,
        country="us"
    )
    
    # Test 1: Get job count
    print("\n=== Test 1: Get Job Count ===")
    count = client.get_job_count("data engineer")
    print(f"Total jobs available: {count}")
    
    # Test 2: Fetch jobs (2 pages, 10 per page = 20 jobs)
    print("\n=== Test 2: Fetch Jobs ===")
    jobs = client.search_jobs("data engineer", max_pages=2, results_per_page=10)
    print(f"Fetched {len(jobs)} jobs")
    
    # Test 3: Display first job
    if jobs:
        print("\n=== Test 3: Sample Job ===")
        job = jobs[0]
        print(f"Title: {job.get('title')}")
        print(f"Company: {job.get('company', {}).get('display_name')}")
        print(f"Location: {job.get('location', {}).get('display_name')}")
        print(f"Salary: ${job.get('salary_min', 0):,.2f} - ${job.get('salary_max', 0):,.2f}")
    
    print(f"\n=== Summary ===")
    print(f"Total API requests made: {client.request_count}")

if __name__ == "__main__":
    main()