import argparse
import json
import asyncio
import os
import sys 

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
    try:
        urls = scraper.scrape_menu_pages()
        scraper.save_urls(urls)
    except KeyboardInterrupt:
        print("\n[INFO] KeyboardInterrupt detected. Saving collected URLs and shutting down.")
        scraper.save_urls(scraper.all_scraped_urls) 
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during URL scraping: {e}")
        scraper.save_urls(scraper.all_scraped_urls)
        sys.exit(1)
    finally:
        scraper.shutdown()


def run_scrape_details():
    scraper = Scraper()
    print("[INFO] Scraping listing details from saved URLs...")
    try:
        scraper.process_listings_from_json(URLS_OUTPUT_PATH, DETAILS_OUTPUT_PATH)
    except KeyboardInterrupt:
        print("\n[INFO] KeyboardInterrupt detected. Any unsaved details have been flushed to CSV.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during details scraping: {e}")
        sys.exit(1) 
    finally:
        scraper.shutdown()


def run_screenshot_upload():
    if not os.path.exists(URLS_OUTPUT_PATH):
        print(f"[ERROR] URLS_OUTPUT_PATH not found: {URLS_OUTPUT_PATH}. Please run 'scrape_urls' first.")
        sys.exit(1)

    with open(URLS_OUTPUT_PATH, "r", encoding="utf-8") as f:
        urls = json.load(f)

    uploader = ScreenshotUploader()
    try:
        asyncio.run(uploader.run(urls))
    except KeyboardInterrupt:
        print("\n[INFO] Screenshot upload interrupted.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during screenshot upload: {e}")
        sys.exit(1)


def run_retry_screenshots(failed_csv):
    uploader = ScreenshotUploader()
    try:
        asyncio.run(uploader.retry_failed_screenshots(failed_csv))
    except KeyboardInterrupt:
        print("\n[INFO] Retrying screenshot upload interrupted.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during retry screenshot upload: {e}")
        sys.exit(1)


def run_clean_data():
    """Initializes and runs the data cleaning process."""
    print("[INFO] Cleaning scraped data...")
    try:
        cleaner = DataCleaner()
        cleaner.load_data() 
        cleaner.clean_data()
        cleaner.save_cleaned_data()
        print("[INFO] Data cleaning completed successfully.")
    except Exception as e:
        print(f"[ERROR] Data cleaning failed: {e}")
        sys.exit(1)


def run_full_pipeline():
    """Runs the entire pipeline from scraping to cleaning."""
    print("[INFO] Running full scraping and cleaning pipeline...")
    try:
        run_scrape_urls()
        run_scrape_details()
        run_clean_data()
        print("[INFO] Full pipeline completed.")
    except KeyboardInterrupt:
        print("\n[INFO] Full pipeline interrupted. Shutting down gracefully.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during the full pipeline: {e}")
        sys.exit(1)


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
            sys.exit(1)
        run_retry_screenshots(args.failed_csv)
    elif args.task == "clean_data":
        run_clean_data()
    elif args.task == "full_pipeline":
        run_full_pipeline()


if __name__ == "__main__":
    main()