import os

BASE_URL = "https://onehousing.vn"
START_URL = f"{BASE_URL}/nha-dat-ban?"
OUTPUT_DIR = "data"

URLS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_urls.json")
DETAILS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_details.csv")
CLEANED_DETAILS_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "listing_details_cleaned.xlsx")

IMAGE_MAP_CSV_PATH = os.path.join(OUTPUT_DIR, "image_map.csv")
SCREENSHOT_DIR = os.path.join(OUTPUT_DIR, "screenshots")

MAX_RETRIES = 5
RETRY_DELAY = 2
MAX_WORKERS = 4

LOG_LEVEL = "INFO"

TOTAL_PAGES = 466

SELENIUM_HEADLESS = True
SELENIUM_WAIT_TIME = 5
DRIVER_POOL_SIZE = 3

CLOUDINARY_CONFIG = {
    "cloud_name": "dlsut5knb",
    "api_key": "132284835379582",
    "api_secret": "mAnbpxUnA1R8MWhrVvLeyKqjssQ"
}