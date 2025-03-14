import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URL of the SEC SIC code list
SEC_URL = "https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list"

# User agent to comply with SEC's EDGAR fair access policy
USER_AGENT = "Findata Academic Project devarapalli.n@northeastern.edu"

# Constants for retry mechanism
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
TIMEOUT = 30

def create_session():
    """Create a requests session with retry mechanism and proper headers"""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    })
    return session

def validate_sic_data(sic_data):
    """Validate scraped SIC data"""
    if not sic_data:
        raise ValueError("No SIC data was scraped")
    
    required_fields = {'sic_code', 'industry_name'}
    for entry in sic_data:
        if not all(field in entry for field in required_fields):
            raise ValueError(f"Missing required fields in entry: {entry}")
        if not entry['sic_code'].strip() or not entry['industry_name'].strip():
            raise ValueError(f"Empty values in entry: {entry}")

def scrape_sic_codes():
    """Scrape SIC codes and industry names from the SEC website"""
    logger.info("Starting to scrape SIC codes from SEC website...")
    
    session = create_session()
    sic_data = []
    
    try:
        # Add a delay to comply with SEC rate limits
        time.sleep(0.1)
        
        # Get the page content
        logger.info(f"Fetching content from {SEC_URL}")
        response = session.get(SEC_URL, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table containing SIC codes using multiple selectors
        table = None
        selectors = ['table.list', 'table.table', 'table.data-table', 'table']
        
        for selector in selectors:
            table = soup.select_one(selector)
            if table and table.find_all('tr'):
                logger.info(f"Found table using selector: {selector}")
                break
        
        if not table:
            raise ValueError("Could not find the SIC codes table on the page")
        
        # Extract data from the table
        rows = table.find_all('tr')
        if not rows:
            raise ValueError("No rows found in the table")

        # Check if the table has at least 2 columns 
        header_row = rows[0].find_all(['th', 'td'])
        if len(header_row) < 2:
            raise ValueError("Invalid table structure: insufficient columns")
        
        # Skip the header row
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) >= 2:
                sic_code = cells[0].text.strip()
                industry_name = cells[1].text.strip()
                
                if sic_code and industry_name:  # Only add if both values are non-empty
                    sic_data.append({
                        'sic_code': sic_code,
                        'industry_name': industry_name
                    })
        
        # Validate scraped data
        validate_sic_data(sic_data)
        
        # Convert to DataFrame
        df = pd.DataFrame(sic_data)
        
        # Create data directory if it doesn't exist
        os.makedirs('data/sic_codes', exist_ok=True)
        
        # Save to CSV
        csv_path = 'data/sic_codes/sic_codes.csv'
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Successfully scraped {len(df)} SIC codes and saved to {csv_path}")
        return csv_path
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making request to SEC website: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing SEC website content: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during SIC code scraping: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    try:
        scrape_sic_codes()
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise