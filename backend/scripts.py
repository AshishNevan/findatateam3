import os
import time
import requests
from snowflake.connector import connect
from dotenv import load_dotenv
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import logging

SEC_URL_TEMPLATE = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
USER_AGENT = "Findata Academic Project devarapalli.n@northeastern.edu"
MAX_RETRIES = 3
RETRY_DELAY = 60


def download_with_retry(year, quarter, logger=None):
    """Download SEC data with retry logic."""
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"Downloading SEC data for {year}Q{quarter}")
    sec_url = SEC_URL_TEMPLATE.format(year=year, quarter=quarter)
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )

    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": USER_AGENT})

    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
            response = session.get(sec_url, timeout=30)
            response.raise_for_status()

            if response.content[:4] != b"PK\x03\x04":
                raise ValueError("Invalid ZIP file header")

            os.makedirs(f"./data/", exist_ok=True)
            with open(f"./data/{year}q{quarter}.zip", "wb") as file:
                for chunk in response.iter_content(chunk_size=128):
                    file.write(chunk)

            logger.info(f"Successfully downloaded SEC data for {year}Q{quarter}")

            response = session.get("https://www.sec.gov/include/ticker.txt")
            with open(f"./data/ticker.txt", "wb") as file:
                for chunk in response.iter_content(chunk_size=128):
                    file.write(chunk)
            logger.info(f"Successfully downloaded ticker.txt")
            return True

        except requests.HTTPError as e:
            if e.response.status_code == 429:
                logger.info(f"Rate limited. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Max retries exceeded: {e}")
                return False


# def download_and_extract(year, quarter):
#     """Download and extract SEC data for a specific year and quarter if not already in S3."""

#     try:
#         content = download_with_retry(year, quarter)
#         output_dir = f"./data/{year}_Q{quarter}"

#         with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
#             zip_ref.extractall(output_dir)

#         logger.info(
#             f"Successfully downloaded and extracted SEC data for {year}Q{quarter}"
#         )

#     except zipfile.BadZipFile:
#         logger.error("Downloaded file is not a valid ZIP - possible rate limit page")
#         with open(f"./data/{year}_Q{quarter}_error.html", "wb") as f:
#             f.write(content)
#         raise


def load_data(year=2023, quarter=4, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

    logger.info(f"Loading data for {year}Q{quarter}")
    CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS json_{year}Q{quarter} (
        json_data VARIANT
    );
    """
    CREATE_FILE_FORMAT = """
    CREATE FILE FORMAT IF NOT EXISTS my_json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;
    """
    CREATE_STAGE = """CREATE STAGE IF NOT EXISTS json_stage
    FILE_FORMAT = my_json_format;
    """

    load_dotenv()
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WH"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cur = conn.cursor()

    cur.execute(CREATE_TABLE)
    logger.info("Created table")
    cur.execute(CREATE_FILE_FORMAT)
    logger.info("Created file format")
    cur.execute(CREATE_STAGE)
    logger.info("Created stage")
    json_directory = Path(f"./backend/exportfiles/{year}q{quarter}")
    logger.info(f"Uploading data to stage {year}q{quarter}")
    cur.execute(f"PUT file://{json_directory}/* @json_stage/{year}q{quarter}/")
    logger.info("Uploaded data to stage")
    cur.execute(
        f"""
                COPY INTO json_{year}Q{quarter} (json_data)
                FROM @json_stage/{year}q{quarter}
                FILE_FORMAT = (FORMAT_NAME = my_json_format)
                ON_ERROR = 'CONTINUE';
            """
    )
    logger.info("Copied data into table")
    cur.close()
    conn.close()


if __name__ == "__main__":
    # load_data(2023, 1)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    download_with_retry(2024, 4, logger)
