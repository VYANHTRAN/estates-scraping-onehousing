import os
import json
import csv
import time
import random

import requests
from bs4 import BeautifulSoup

from queue import Queue
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from src.config import (
    BASE_URL, START_URL, OUTPUT_DIR, URLS_OUTPUT_PATH,
    DETAILS_OUTPUT_PATH, MAX_RETRIES, RETRY_DELAY,
    MAX_WORKERS, LOG_LEVEL, TOTAL_PAGES
)

class DriverPool:
    def __init__(self, max_workers, user_agent_generator):
        self.pool = Queue(maxsize=max_workers)
        self.max_workers = max_workers
        self.ua = user_agent_generator
        self._init_pool()

    def _init_pool(self):
        for _ in range(self.max_workers):
            user_agent = self._get_random_user_agent()
            options = Options()
            options.add_argument(f"user-agent={user_agent}")
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(options=options)
            self.pool.put(driver)

    def _get_random_user_agent(self):
        try:
            return self.ua.random
        except Exception:
            fallback_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...Chrome/123.0.0.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; rv:115.0)...Firefox/115.0"
            ]
            return random.choice(fallback_agents)

    def acquire(self):
        return self.pool.get()

    def release(self, driver):
        self.pool.put(driver)

    def close_all(self):
        while not self.pool.empty():
            driver = self.pool.get()

            try:
                driver.quit()
            except Exception:
                pass


class Scraper:
    def __init__(self):
        # Initialize user agent generator and driver pool
        self.ua = UserAgent()
        self.driver_pool = DriverPool(MAX_WORKERS, self.ua)

        # Define fieldnames for CSV output
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]

        # Ensure the output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def log(self, message, level="INFO"):
        """
        Logs messages.
        """
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        config_level_idx = levels.index(LOG_LEVEL.upper()) if LOG_LEVEL.upper() in levels else 1
        message_level_idx = levels.index(level.upper()) if level.upper() in levels else 1

        if message_level_idx >= config_level_idx:
            print(f"[{level}] {message}")

    def get_listing_urls(self, html):
        """
        Extracts listing URLs from the provided HTML content.
        """
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('a[data-role="property-card"]')
        urls = [
            BASE_URL + card.get("href") if card.get("href") and not card.get("href").startswith("http") else card.get("href")
            for card in cards if card.get("href")
        ]
        return urls

    def scrape_menu_pages(self):
        """
        Scrapes URLs of all listing pages from the paginated menu.
        """
        all_links = set()

        for page_num in tqdm(range(1, TOTAL_PAGES + 1), desc="Scraping menu pages"):
            url = f"{START_URL}page={page_num}"

            for retry in range(MAX_RETRIES):
                try:
                    headers = {"User-Agent": self.driver_pool._get_random_user_agent()}
                    response = requests.get(url, headers=headers)

                    if response.status_code != 200:
                        raise Exception(f"Status code {response.status_code}")

                    links = self.get_listing_urls(response.text)
                    self.log(f"Extracted {len(links)} links from page {page_num}", "DEBUG")
                    all_links.update(links)
                    break

                except Exception as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} fetching page {page_num} due to: {e}", "WARN")
                    time.sleep(RETRY_DELAY)
            else:
                self.log(f"Failed to fetch page {page_num} after retries", "ERROR")

        return all_links

    def save_urls(self, urls):
        """
        Saves the collected URLs to a JSON file.
        """
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls), f, ensure_ascii=False, indent=2)

        self.log(f"Saved {len(urls)} URLs to {URLS_OUTPUT_PATH}", "INFO")

    def extract_listing_details(self, url):
        """
        Extracts details from a specific listing page.
        """
        driver = self.driver_pool.acquire()
        wait = WebDriverWait(driver, 5)
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))

        try:
            driver.get(url)

            # Helper functions for safe extraction of text and attributes
            def safe_text(by, selector):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text
                except (TimeoutException, NoSuchElementException):
                    return None

            def safe_attr(by, selector, attr):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).get_attribute(attr)
                except (TimeoutException, NoSuchElementException):
                    return None

            # Initialize data dictionary with extracted fields
            data = {
                "listing_title": safe_text(By.XPATH, '//*[@id="detail_title"]'),
                "property_id": safe_text(By.CSS_SELECTOR, '#container-property div:nth-child(5) div.flex.cursor-pointer p'),
                "total_price": safe_text(By.XPATH, '//*[@id="total-price"]'),
                "unit_price": safe_text(By.XPATH, '//*[@id="unit-price"]'),
                "property_url": safe_attr(By.XPATH, '//link[@rel="canonical"]', "href"),
                "alley_width": safe_text(By.XPATH, '//*[@id="overview_content"]//div[@data-impression-index="1"]'),
                "image_url": None,
                "city": None,
                "district": None,
                "features": [],
                "property_description": []
            }

            # Extract image URL
            try:
                image_path = driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = image_path.get_attribute("imagesrcset")
                image_url = image_src.split(',')[0].strip().split(' ')[0]
                data['image_url'] = image_url
            except (NoSuchElementException, json.JSONDecodeError):
                self.log("Could not decode retrieve main image", "DEBUG")

            # Extract breadcrumb information for city and district
            try:
                script = driver.find_element(By.XPATH, '/html/head/script[2]').get_attribute("innerHTML")
                breadcrumb_data = json.loads(script)

                for item in breadcrumb_data.get("itemListElement", []):
                    if item.get("position") == 2:
                        data["city"] = item.get("name")
                    elif item.get("position") == 3:
                        data["district"] = item.get("name")
            except (NoSuchElementException, json.JSONDecodeError):
                self.log("Could not decode breadcrumb JSON", "DEBUG")

            # Extract features
            try:
                features = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="key-feature-item"]')))

                for ele in features:
                    try:
                        title = ele.find_element(By.XPATH, './/*[@id="item_title"]').text
                        text = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]').text

                        if title and text:
                            data["features"].append(f"{title.strip()}: {text.strip()}")
                    except NoSuchElementException:
                        continue
            except (TimeoutException, NoSuchElementException):
                pass

            # Extract description
            try:
                property_description = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, 'ul[aria-label="description-heading"].relative li')
                ))
                data["property_description"] = [li.get_attribute("data-testid") for li in property_description if li.get_attribute("data-testid")]
            except (TimeoutException, NoSuchElementException):
                pass

            return data
        finally:
            self.driver_pool.release(driver)

    def save_details_to_csv(self, listing, filename):
        """
        Saves listing details to a CSV file.
        """
        listing["features"] = ": ".join(listing.get("features", []))
        listing["property_description"] = ". ".join(listing.get("property_description", []))

        file_exists = os.path.isfile(filename)

        with open(filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)

            if not file_exists:
                writer.writeheader()
            writer.writerow(listing)

    def scrape_with_retries(self, url):
        """
        Retries scraping a listing page up to a maximum number of attempts.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return self.extract_listing_details(url)
            except Exception as e:
                self.log(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}", "WARN")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

        self.log(f"All attempts failed for {url}", "ERROR")
        return None

    def process_listings_from_json(self, json_path, output_csv=DETAILS_OUTPUT_PATH):
        """
        Reads a JSON file of listing URLs and scrapes details for each URL.
        Saves the results to a CSV file.
        """
        if not os.path.exists(json_path):
            self.log(f"JSON file not found: {json_path}", "ERROR")
            return

        with open(json_path, "r", encoding="utf-8") as f:
            try:
                urls = json.load(f)
            except json.JSONDecodeError as e:
                self.log(f"Invalid JSON format: {e}", "ERROR")
                return

        self.log(f"Starting parallel scrape for {len(urls)} listings...", "INFO")
        file_exists = os.path.isfile(output_csv)

        with open(output_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)

            if not file_exists:
                writer.writeheader()

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.scrape_with_retries, url): url for url in urls}

                for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping listings"):
                    url = futures[future]

                    try:
                        result = future.result()
                        if result:
                            self.save_details_to_csv(result, output_csv)
                    except Exception as exc:
                        self.log(f"Unexpected error with {url}: {exc}", "ERROR")


    def shutdown(self):
        """
        Shuts down the driver pool and releases all resources.
        """
        self.driver_pool.close_all()