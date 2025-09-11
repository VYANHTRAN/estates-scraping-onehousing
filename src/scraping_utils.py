import os
import json
import csv
import time
import random
import sys
import threading 

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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, SessionNotCreatedException

from src.config import *

class DriverPool:
    def __init__(self, max_workers, user_agent_generator):
        self.pool = Queue(maxsize=max_workers)
        self.max_workers = max_workers
        self.ua = user_agent_generator
        self._init_pool()
        self.drivers_in_use = 0 

    def _init_pool(self):
        for _ in range(self.max_workers):
            self._add_new_driver()

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

    def _add_new_driver(self):
        user_agent = self._get_random_user_agent()
        options = Options()
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--enable-unsafe-swiftshader")
        options.add_experimental_option('excludeSwitches', ['enable-logging']) 
        
        try:
            driver = webdriver.Chrome(options=options)
            self.pool.put(driver)
        except SessionNotCreatedException as e:
            print(f"[ERROR] Could not create a Chrome session. Please ensure chromedriver is compatible with your Chrome browser version. Error: {e}", file=sys.stderr)
            if self.pool.empty(): 
                raise RuntimeError("Failed to initialize any WebDriver instances.")

    def acquire(self):
        driver = self.pool.get()
        self.drivers_in_use += 1
        return driver

    def release(self, driver):
        if driver: 
            self.pool.put(driver)
        self.drivers_in_use -= 1


    def close_all(self):
        for driver in self.all_drivers: # Iterate through all drivers and quit them
            try:
                driver.quit()
            except Exception as e:
                print(f"[ERROR] Error quitting driver: {e}")
        self.all_drivers.clear()
        while not self.pool.empty():
            self.pool.get()
        self.drivers_in_use = 0


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.driver_pool = DriverPool(MAX_WORKERS, self.ua)
        self.all_scraped_urls = set()
        self.details_buffer = []
        self.stop_requested = threading.Event() 
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Initialize CSV writer for details scraping with thread-safe lock 
        self.details_csv_file = None
        self.details_csv_writer = None
        self.details_csv_lock = threading.Lock() 

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
            if self.stop_requested.is_set(): 
                self.log("Stop requested. Exiting URL scraping.", "INFO")
                break

            url = f"{START_URL}page={page_num}"
            consecutive_http_errors = 0
            consecutive_empty_pages = 0

            for retry in range(MAX_RETRIES):
                if self.stop_requested.is_set():
                    break

                try:
                    headers = {"User-Agent": self.driver_pool._get_random_user_agent()}
                    response = requests.get(url, headers=headers, timeout=10)

                    if 400 <= response.status_code < 600:
                        consecutive_http_errors += 1
                        if consecutive_http_errors >= 3:
                            self.log(f"Critical: {consecutive_http_errors} consecutive HTTP errors (last: {response.status_code}) encountered while fetching {url}. Stopping URL scraping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception(f"Status code {response.status_code}")
                    elif response.text == None:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.log(f"Critical: {consecutive_empty_pages} consecutive empty pages encountered while fetching {url}. Stopping URL scraping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception("Empty page content")

                    consecutive_http_errors = 0
                    consecutive_empty_pages = 0
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

            if self.stop_requested.is_set():
                break
        return self.all_scraped_urls

    def save_urls(self, urls_to_save):
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls_to_save), f, ensure_ascii=False, indent=2)
        self.log(f"Saved {len(urls_to_save)} URLs to {URLS_OUTPUT_PATH}", "INFO")

    def _initialize_details_csv(self, output_csv, append=False):
        """Initializes the CSV writer for listing details."""
        with self.details_csv_lock:
            file_exists = os.path.isfile(output_csv) and append
            self.details_csv_file = open(output_csv, "a" if append else "w", newline="", encoding="utf-8")
            self.details_csv_writer = csv.DictWriter(self.details_csv_file, fieldnames=self.fieldnames)

            if not file_exists:
                self.details_csv_writer.writeheader()

    def _close_details_csv(self):
        """Closes the CSV file if it's open."""
        with self.details_csv_lock:
            if self.details_csv_file:
                self.details_csv_file.close()
                self.details_csv_file = None
                self.details_csv_writer = None

    def extract_listing_details(self, url):
        if self.stop_requested.is_set():
            return None

        driver = None 

        try:
            driver = self.driver_pool.acquire()

            if self.stop_requested.is_set():
                return None

            driver.get(url)
            wait = WebDriverWait(driver, 5)
            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))

            # -- Helper functions for safe extraction of text and attributes -- 
            def safe_text(by, selector, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text
                except (TimeoutException, NoSuchElementException, WebDriverException): 
                    return None

            def safe_attr(by, selector, attr, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).get_attribute(attr)
                except (TimeoutException, NoSuchElementException, WebDriverException):
                    return None
            # -- End of helper functions --

            # Start extracting data
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
            if driver:
                try:
                    driver.quit() 
                except Exception as quit_e:
                    self.log(f"Error quitting crashed driver: {quit_e}", "ERROR")
                self.driver_pool.drivers_in_use -= 1 
                self.driver_pool._add_new_driver() 
            return None
        except Exception as e:
            self.log(f"Unexpected error in extract_listing_details for {url}: {e}", "ERROR")
            return None
        finally:
            if driver and not self.stop_requested.is_set(): # Only release if not stopping
                self.driver_pool.release(driver)
            elif driver and self.stop_requested.is_set(): # If stopping, quit the driver
                try:
                    driver.quit()
                    self.driver_pool.drivers_in_use -= 1 
                except Exception as quit_e:
                    self.log(f"Error quitting driver during shutdown: {quit_e}", "ERROR")

    def save_details_to_csv(self, listing):
        """
        Saves listing details to the initialized CSV writer.
        This function no longer takes 'filename' but writes to the instance's writer.
        """
        if self.details_csv_writer:
            listing_copy = listing.copy()
            listing_copy["features"] = ": ".join(listing_copy.get("features", []))
            listing_copy["property_description"] = ". ".join(listing_copy.get("property_description", []))
            with self.details_csv_lock: 
                self.details_csv_writer.writerow(listing_copy)
                self.details_csv_file.flush()
        else:
            self.log("CSV writer not initialized. Cannot save details.", "ERROR")


    def scrape_with_retries(self, url):
        """
        Retries scraping a listing page up to a maximum number of attempts.
        Also checks for stop_requested.
        """
        if self.stop_requested.is_set():
            return None

        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_requested.is_set():
                return None
            try:
                result = self.extract_listing_details(url)
                if result is not None:
                    return result
            except Exception as e:
                self.log(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}", "WARN")
                if attempt < MAX_RETRIES and not self.stop_requested.is_set():
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
                try:
                    for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping listings"):
                        if self.stop_requested.is_set():
                            self.log("Stop requested. Exiting details scraping.", "INFO")
                            # Cancel remaining futures
                            for f in futures:
                                f.cancel()
                            break 
                        
                        url = futures[future]
                        try:
                            result = future.result()
                            if result:
                                self.save_details_to_csv(result)
                        except Exception as exc:
                            self.log(f"Unexpected error with {url}: {exc}", "ERROR")
                finally:
                    executor.shutdown(wait=True) 
        except KeyboardInterrupt:
            self.log("KeyboardInterrupt detected. Stopping details scraping.", "INFO")
            self.stop_requested.set()

            for f in futures:
                f.cancel()

            executor.shutdown(wait=False, cancel_futures=True)  
            self.shutdown()
            return
        finally:
            self._close_details_csv()
            self.log("Details CSV file closed.", "INFO")


    def shutdown(self):
        self.log("Shutting down scraper components...", "INFO")
        self.driver_pool.close_all()
        self._close_details_csv()
        self.log("Scraper shutdown complete.", "INFO")