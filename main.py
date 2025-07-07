import argparse
import json
import asyncio

from src.scraping_utils import Scraper
from src.screenshot_uploader import ScreenshotUploader
from src.cleaning_utils import DataCleaner
from src.config import (
    URLS_OUTPUT_PATH,
    DETAILS_OUTPUT_PATH
)


def run_scrape_urls():
    scraper = Scraper()
    print("[INFO] Scraping listing URLs...")
    urls = scraper.scrape_menu_pages()
    scraper.save_urls(urls)
    scraper.shutdown()


def run_scrape_details():
    scraper = Scraper()
    print("[INFO] Scraping listing details from saved URLs...")
    scraper.process_listings_from_json(URLS_OUTPUT_PATH, DETAILS_OUTPUT_PATH)
    scraper.shutdown()


def run_screenshot_upload():
    with open(URLS_OUTPUT_PATH, "r", encoding="utf-8") as f:
        urls = json.load(f)

    uploader = ScreenshotUploader()
    asyncio.run(uploader.run(urls))


def run_retry_screenshots(failed_csv):
    uploader = ScreenshotUploader()
    asyncio.run(uploader.retry_failed_screenshots(failed_csv))


def run_clean_data():
    """Initializes and runs the data cleaning process."""
    print("[INFO] Cleaning scraped data...")
    try:
        cleaner = DataCleaner()
        cleaner.clean_data()
        cleaner.save_cleaned_data()
        print("[INFO] Data cleaning completed successfully.")
    except Exception as e:
        print(f"[ERROR] Data cleaning failed: {e}")


def run_full_pipeline():
    """Runs the entire pipeline from scraping to cleaning."""
    print("[INFO] Running full scraping and cleaning pipeline...")
    run_scrape_urls()
    run_scrape_details()
    run_screenshot_upload()
    run_clean_data()  # Add the cleaning step to the end
    print("[INFO] Full pipeline completed.")


def main():
    parser = argparse.ArgumentParser(description="Run scraping and cleaning tasks.")
    parser.add_argument(
        "task",
        choices=[
            "scrape_urls",
            "scrape_details",
            "upload_screenshots",
            "retry_screenshots",
            "clean_data",
            "full_pipeline",
        ],
        help="The task to run.",
    )
    parser.add_argument(
        "--failed_csv",
        type=str,
        help="Path to CSV of failed screenshots (required for retry_screenshots).",
    )

    args = parser.parse_args()

    if args.task == "scrape_urls":
        run_scrape_urls()
    elif args.task == "scrape_details":
        run_scrape_details()
    elif args.task == "upload_screenshots":
        run_screenshot_upload()
    elif args.task == "retry_screenshots":
        if not args.failed_csv:
            print("[ERROR] --failed_csv is required for retry_screenshots.")
            return
        run_retry_screenshots(args.failed_csv)
    elif args.task == "clean_data":  # Add handler for the new task
        run_clean_data()
    elif args.task == "full_pipeline":
        run_full_pipeline()


if __name__ == "__main__":
    main()