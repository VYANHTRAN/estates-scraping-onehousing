import os
import csv
import asyncio
import cloudinary
import cloudinary.uploader

from pathlib import Path
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright
from src.config import (
    CLOUDINARY_CONFIG,
    MAX_RETRIES,
    RETRY_DELAY,
    IMAGE_MAP_CSV_PATH,
    SCREENSHOT_DIR
)

cloudinary.config(**CLOUDINARY_CONFIG)


class ScreenshotUploader:
    def __init__(self, concurrency=5):
        self.screenshot_dir = SCREENSHOT_DIR
        self.image_map_csv = IMAGE_MAP_CSV_PATH
        self.failed_screenshots = []
        self.semaphore = asyncio.Semaphore(concurrency)
        Path(self.screenshot_dir).mkdir(parents=True, exist_ok=True)

    async def screenshot_and_extract_id(self, page, url):
        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_selector('#container-property', timeout=10000)

                locator = page.locator('#container-property div:nth-child(5) div.cursor-pointer p')
                property_id = (await locator.inner_text()).strip()

                screenshot_path = os.path.join(self.screenshot_dir, f"{property_id}.jpeg")
                await page.screenshot(path=screenshot_path, full_page=True, type="jpeg", quality=70)

                return property_id, screenshot_path
            except Exception:
                await asyncio.sleep(RETRY_DELAY)
        return None, None

    async def upload_to_cloudinary(self, image_path, property_id):
        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: cloudinary.uploader.upload(
                    image_path,
                    public_id=f"listings/{property_id}",
                    overwrite=True
                )
            )
            return result.get("secure_url")
        except Exception as e:
            print(f"[ERROR] Upload failed for {property_id}: {e}")
            return None

    async def process_single(self, browser, url, writer):
        async with self.semaphore:
            page = await browser.new_page()

            try:
                property_id, screenshot_path = await self.screenshot_and_extract_id(page, url)

                if not property_id or not screenshot_path:
                    print(f"[FAILED] Screenshot failed: {url}")
                    self.failed_screenshots.append({"url": url, "reason": "screenshot_failed"})
                    return

                image_url = await self.upload_to_cloudinary(screenshot_path, property_id)
                os.remove(screenshot_path)

                if image_url:
                    writer.writerow({"property_id": property_id, "screenshot_url": image_url})
                    print(f"[INFO] Processed: {property_id}")
                else:
                    self.failed_screenshots.append({"url": url, "property_id": property_id, "reason": "upload_failed"})
                    print(f"[FAILED] Upload failed: {property_id}")
            finally:
                await page.close()

    async def run(self, urls, append=False):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            file_mode = "a" if append else "w"
            file_exists = os.path.exists(self.image_map_csv)

            with open(self.image_map_csv, file_mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["property_id", "screenshot_url"])

                if not append or not file_exists:
                    writer.writeheader()

                tasks = [self.process_single(browser, url, writer) for url in urls]
                await tqdm.gather(*tasks)

            await browser.close()

        if self.failed_screenshots:
            failed_path = os.path.splitext(self.image_map_csv)[0] + "_failures.csv"

            with open(failed_path, "w", newline="", encoding="utf-8") as f:
                fieldnames = ["url", "property_id", "reason"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for failure in self.failed_screenshots:
                    writer.writerow({
                        "url": failure.get("url", ""),
                        "property_id": failure.get("property_id", ""),
                        "reason": failure["reason"]
                    })

            print(f"[INFO] {len(self.failed_screenshots)} failures saved to: {failed_path}")

    async def retry_failed_screenshots(self, failed_csv_path):
        if not os.path.exists(failed_csv_path):
            print(f"[ERROR] Failed screenshot CSV not found: {failed_csv_path}")
            return

        urls_to_retry = []

        with open(failed_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("url"):
                    urls_to_retry.append(row["url"])

        if not urls_to_retry:
            print("[INFO] No failed URLs to retry.")
            return

        print(f"[INFO] Retrying {len(urls_to_retry)} failed screenshots...")
        await self.run(urls_to_retry, append=True)