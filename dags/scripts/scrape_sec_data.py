import requests
import zipfile
import io
import os
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SEC URL template for financial statement data sets
SEC_URL_TEMPLATE = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"

# User agent to comply with SEC's EDGAR fair access policy
USER_AGENT = "Findata Academic Project devarapalli.n@northeastern.edu"

# Constants for retry mechanism
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_DELAY = 60
TIMEOUT = 30

# Required files in the SEC data set
REQUIRED_FILES = ['sub.txt', 'num.txt', 'pre.txt', 'tag.txt']

def create_session():
    """Create a requests session with retry mechanism and proper headers"""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=frozenset(['GET'])
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    })
    return session

def download_with_retry(year, quarter):
    """Download SEC data with retries and SEC compliance."""
    sec_url = SEC_URL_TEMPLATE.format(year=year, quarter=quarter)
    session = create_session()
    
    logger.info(f"Attempting to download SEC data for {year}Q{quarter} from {sec_url}")
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.get(sec_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Validate ZIP header
            if response.content[:4] != b'PK\x03\x04':
                raise ValueError("Invalid ZIP file header")
            
            logger.info(f"Successfully downloaded SEC data for {year}Q{quarter}")
            return response.content
            
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limited. Retrying in {RETRY_DELAY} seconds... (Attempt {attempt+1}/{MAX_RETRIES+1})")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"HTTP error: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error downloading SEC data: {str(e)}")
            raise
    
    raise Exception("Max retries exceeded")

def scrape_sec_data(year, quarter, output_dir=None):
    """Download and extract SEC data for a specific year and quarter.
    
    Args:
        year (int): The year to download data for
        quarter (int): The quarter to download data for (1-4)
        output_dir (str, optional): Directory to extract files to. If None, uses '/data/{year}_Q{quarter}'
    
    Returns:
        str: Path to the directory containing extracted files
    """
    if output_dir is None:
        output_dir = f'/data/{year}_Q{quarter}'
    
    logger.info(f"Starting to scrape SEC data for {year}Q{quarter}")
    
    try:
        # Download the ZIP file
        content = download_with_retry(year, quarter)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract the ZIP file
        logger.info(f"Extracting SEC data to {output_dir}")
        with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
            zip_ref.extractall(output_dir)
        
        # Verify all required files were extracted
        missing_files = []
        for filename in REQUIRED_FILES:
            file_path = os.path.join(output_dir, filename)
            if not os.path.exists(file_path):
                missing_files.append(filename)
        
        if missing_files:
            raise ValueError(f"Missing required files after extraction: {', '.join(missing_files)}")
        
        logger.info(f"Successfully downloaded and extracted SEC data for {year}Q{quarter} to {output_dir}")
        return output_dir
        
    except zipfile.BadZipFile:
        logger.error("Downloaded file is not a valid ZIP - possible rate limit page")
        error_file = f'{output_dir}_error.html'
        with open(error_file, 'wb') as f:
            f.write(content)
        logger.error(f"Saved error response to {error_file}")
        raise
    except Exception as e:
        logger.error(f"Error scraping SEC data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Example usage
        year = 2023
        quarter = 4
        scrape_sec_data(year, quarter)
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise