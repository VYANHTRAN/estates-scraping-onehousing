import os

BASE_URL = "https://onehousing.vn"
START_URL = f"{BASE_URL}/nha-dat-ban?"
OUTPUT_DIR = "data"

URLS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_urls.json")
DETAILS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_details.csv")
CLEANED_DETAILS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_details_cleaned.xlsx")

MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_WORKERS = 1

LOG_LEVEL = "INFO"

TOTAL_PAGES = 5

SELENIUM_WAIT_TIME = 5
DRIVER_POOL_SIZE = 1