import os
import json
import csv
import time
import random
import sys 

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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

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
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0"
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
            except Exception as e:
                self.log(f"Error quitting driver: {e}", "ERROR")


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.driver_pool = DriverPool(MAX_WORKERS, self.ua)
        self.all_scraped_urls = set() 
        self.details_buffer = [] 
        self.stop_requested = False 
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Initialize CSV writer for details scraping
        self.details_csv_file = None
        self.details_csv_writer = None

    def log(self, message, level="INFO"):
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"] 
        config_level_idx = levels.index(LOG_LEVEL.upper()) if LOG_LEVEL.upper() in levels else 1
        message_level_idx = levels.index(level.upper()) if level.upper() in levels else 1

        if message_level_idx >= config_level_idx:
            print(f"[{level}] {message}")

    def get_listing_urls(self, html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('a[data-role="property-card"]')
        urls = [
            BASE_URL + card.get("href") if card.get("href") and not card.get("href").startswith("http") else card.get("href")
            for card in cards if card.get("href")
        ]
        return urls

    def scrape_menu_pages(self):
        for page_num in tqdm(range(1, TOTAL_PAGES + 1), desc="Scraping menu pages"):
            if self.stop_requested:
                self.log("Stop requested. Exiting URL scraping.", "INFO")
                break

            url = f"{START_URL}page={page_num}"
            consecutive_http_errors = 0

            for retry in range(MAX_RETRIES):
                try:
                    headers = {"User-Agent": self.driver_pool._get_random_user_agent()}
                    response = requests.get(url, headers=headers, timeout=10) 

                    if 400 <= response.status_code < 600:
                        consecutive_http_errors += 1
                        if consecutive_http_errors >= 3: 
                            self.log(f"Critical: {consecutive_http_errors} consecutive HTTP errors (last: {response.status_code}) encountered while fetching {url}. Stopping URL scraping.", "CRITICAL")
                            self.stop_requested = True
                            break
                        raise Exception(f"Status code {response.status_code}") 
                    
                    consecutive_http_errors = 0 
                    links = self.get_listing_urls(response.text)
                    self.log(f"Extracted {len(links)} links from page {page_num}", "DEBUG")
                    self.all_scraped_urls.update(links)
                    break

                except requests.exceptions.RequestException as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} fetching page {page_num} due to network/HTTP error: {e}", "WARN")
                    time.sleep(RETRY_DELAY)
                except Exception as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} fetching page {page_num} due to: {e}", "WARN")
                    time.sleep(RETRY_DELAY)
            else:
                self.log(f"Failed to fetch page {page_num} after retries. Moving to next page.", "ERROR")
            
            if self.stop_requested:
                break 
        return self.all_scraped_urls
    
    def save_urls(self, urls_to_save):
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls_to_save), f, ensure_ascii=False, indent=2)
        self.log(f"Saved {len(urls_to_save)} URLs to {URLS_OUTPUT_PATH}", "INFO")

    def _initialize_details_csv(self, output_csv, append=False):
        """Initializes the CSV writer for listing details."""
        file_exists = os.path.isfile(output_csv) and append
        self.details_csv_file = open(output_csv, "a" if append else "w", newline="", encoding="utf-8")
        self.details_csv_writer = csv.DictWriter(self.details_csv_file, fieldnames=self.fieldnames)

        if not file_exists:
            self.details_csv_writer.writeheader()

    def _close_details_csv(self):
        """Closes the CSV file if it's open."""
        if self.details_csv_file:
            self.details_csv_file.close()
            self.details_csv_file = None
            self.details_csv_writer = None

    def extract_listing_details(self, url):
        driver = self.driver_pool.acquire()
        
        # Check if a global stop has been requested
        if self.stop_requested:
            self.driver_pool.release(driver)
            return None

        try:
            driver.get(url)
            driver.get(url)
            wait = WebDriverWait(driver, 5)  # Moved this line up
            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body"))) 

            # Helper functions for safe extraction of text and attributes
            def safe_text(by, selector, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text
                except (TimeoutException, NoSuchElementException):
                    return None

            def safe_attr(by, selector, attr, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).get_attribute(attr)
                except (TimeoutException, NoSuchElementException):
                    return None

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
                image_element = driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = image_element.get_attribute("imagesrcset")
                if image_src:
                    image_url = image_src.split(',')[0].strip().split(' ')[0]
                    data['image_url'] = image_url
            except (NoSuchElementException, WebDriverException):
                self.log(f"Could not retrieve main image for {url}", "DEBUG")

            # Extract breadcrumb information for city and district
            try:
                script_elements = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
                breadcrumb_data = None
                for script_el in script_elements:
                    script_content = script_el.get_attribute("innerHTML")
                    try:
                        json_data = json.loads(script_content)
                        if isinstance(json_data, dict) and json_data.get("@type") == "BreadcrumbList":
                            breadcrumb_data = json_data
                            break
                    except json.JSONDecodeError:
                        continue 

                if breadcrumb_data:
                    for item in breadcrumb_data.get("itemListElement", []):
                        if item.get("position") == 2:
                            data["city"] = item.get("name")
                        elif item.get("position") == 3:
                            data["district"] = item.get("name")
            except (NoSuchElementException, WebDriverException, json.JSONDecodeError):
                self.log(f"Could not decode breadcrumb JSON for {url}", "DEBUG")

            # Extract features
            try:
                features = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="key-feature-item"]')))
                for ele in features:
                    try:
                        title_element = ele.find_element(By.XPATH, './/*[@id="item_title"]')
                        text_element = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]')
                        title = title_element.text
                        text = text_element.text
                        if title and text:
                            data["features"].append(f"{title.strip()}: {text.strip()}")
                    except NoSuchElementException:
                        continue
            except (TimeoutException, NoSuchElementException):
                self.log(f"No key features found for {url}", "DEBUG")

            # Extract description
            try:
                property_description_elements = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, 'ul[aria-label="description-heading"].relative li')
                ))
                data["property_description"] = [li.get_attribute("data-testid") for li in property_description_elements if li.get_attribute("data-testid")]
            except (TimeoutException, NoSuchElementException):
                self.log(f"No property description found for {url}", "DEBUG")

            return data
        except WebDriverException as e:
            self.log(f"WebDriver error for {url}: {e}", "ERROR")
            return None
        except Exception as e:
            self.log(f"Unexpected error in extract_listing_details for {url}: {e}", "ERROR")
            return None
        finally:
            self.driver_pool.release(driver)

    def save_details_to_csv(self, listing):
        """
        Saves listing details to the initialized CSV writer.
        This function no longer takes 'filename' but writes to the instance's writer.
        """
        if self.details_csv_writer:
            listing_copy = listing.copy()
            listing_copy["features"] = ": ".join(listing_copy.get("features", []))
            listing_copy["property_description"] = ". ".join(listing_copy.get("property_description", []))
            self.details_csv_writer.writerow(listing_copy)
            self.details_csv_file.flush() 
        else:
            self.log("CSV writer not initialized. Cannot save details.", "ERROR")


    def scrape_with_retries(self, url):
        """
        Retries scraping a listing page up to a maximum number of attempts.
        Also checks for stop_requested.
        """
        if self.stop_requested:
            return None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = self.extract_listing_details(url)
                if result is not None:
                    return result
            except Exception as e:
                self.log(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}", "WARN")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        self.log(f"All attempts failed for {url}", "ERROR")
        return None

    def process_listings_from_json(self, json_path, output_csv=DETAILS_OUTPUT_PATH):
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
        self._initialize_details_csv(output_csv, append=True) 

        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.scrape_with_retries, url): url for url in urls}

                for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping listings"):
                    if self.stop_requested:
                        self.log("Stop requested. Exiting details scraping.", "INFO")
                        break 
                    
                    url = futures[future]
                    try:
                        result = future.result()
                        if result:
                            self.save_details_to_csv(result) 
                    except Exception as exc:
                        self.log(f"Unexpected error with {url}: {exc}", "ERROR")
        finally:
            self._close_details_csv() 
            self.log("Details CSV file closed.", "INFO")


    def shutdown(self):
        self.driver_pool.close_all()
        self._close_details_csv() 