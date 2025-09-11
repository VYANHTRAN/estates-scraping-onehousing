import os
import json
import csv
import time
import random
import sys
import threading 

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, SessionNotCreatedException

from src.config import *


class DriverPool:
    def __init__(self, user_agent_generator):
        self.driver = None
        self.ua = user_agent_generator
        self._scraper_stop_requested = threading.Event() 
        self._init_driver()

    def _init_driver(self):
        self.log("Initializing WebDriver...", "INFO")
        user_agent = self._get_random_user_agent()
        options = Options()
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--enable-unsafe-swiftshader")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("--window-size=1920,1080")

        try:
            self.driver = webdriver.Chrome(options=options)
            self.log("Successfully initialized WebDriver.", "DEBUG")
        except SessionNotCreatedException as e:
            self.log(f"[ERROR] Could not create a Chrome session. Ensure chromedriver matches your Chrome version. Error: {e}", "CRITICAL")
            raise RuntimeError("Failed to initialize WebDriver.") from e
        except Exception as e:
            self.log(f"[ERROR] An unexpected error occurred while creating a WebDriver: {e}", "CRITICAL")
            raise RuntimeError("Failed to initialize WebDriver.") from e

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
        if self._scraper_stop_requested.is_set():
            raise RuntimeError("Acquire cancelled: Scraper shutdown initiated.")
        if self.driver and self.driver.session_id:
            return self.driver
        else:
            self.log("Driver is not active, attempting to re-initialize.", "WARN")
            self._init_driver()
            return self.driver


    def release(self, driver):
        pass

    def close_all(self):
        self.log("Closing WebDriver instance...", "INFO")
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.log(f"[ERROR] Error quitting WebDriver: {e}", "WARN")
        self.log("WebDriver instance closed.", "INFO")

    def log(self, message, level="INFO"):
        if hasattr(self, '_scraper_log_method'):
            self._scraper_log_method(message, level)
        else:
            levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
            config_level = LOG_LEVEL.upper() if 'LOG_LEVEL' in globals() else "INFO"
            config_level_idx = levels.index(config_level)
            message_level_idx = levels.index(level.upper())
            if message_level_idx >= config_level_idx:
                print(f"[DriverPool-{level}] {message}", file=sys.stderr if level in ["ERROR", "CRITICAL"] else sys.stdout)


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.driver_pool = DriverPool(self.ua)
        self.driver_pool._scraper_log_method = self.log
        self.stop_requested = threading.Event() 
        self.driver_pool._scraper_stop_requested = self.stop_requested 

        self.all_scraped_urls = set()
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        self.details_csv_file = None
        self.details_csv_writer = None
        self.details_csv_lock = threading.Lock() 

    def log(self, message, level="INFO"):
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        config_level_idx = levels.index(LOG_LEVEL.upper()) if LOG_LEVEL.upper() in levels else 1
        message_level_idx = levels.index(level.upper()) if level.upper() in levels else 1
        if message_level_idx >= config_level_idx:
            print(f"[{level}] {message}", file=sys.stderr if level in ["ERROR", "CRITICAL"] else sys.stdout)

    # -------------------- Menu Scraping --------------------
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
                            self.log(f"Critical: {consecutive_http_errors} HTTP errors at {url}. Stopping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception(f"Status code {response.status_code}")
                    elif not response.text:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.log(f"Critical: {consecutive_empty_pages} empty pages at {url}. Stopping.", "CRITICAL")
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
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}", "WARN")
                    if self.stop_requested.is_set():
                        break
                    time.sleep(RETRY_DELAY)
                except Exception as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}", "WARN")
                    if self.stop_requested.is_set():
                        break
                    time.sleep(RETRY_DELAY)
            else:
                self.log(f"Failed to fetch page {page_num} after {MAX_RETRIES} retries.", "ERROR")

            if self.stop_requested.is_set():
                break
        return self.all_scraped_urls

    def save_urls(self, urls_to_save):
        if not urls_to_save:
            self.log("No URLs to save.", "INFO")
            return
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls_to_save), f, ensure_ascii=False, indent=2)
        self.log(f"Saved {len(urls_to_save)} URLs to {URLS_OUTPUT_PATH}", "INFO")

    # -------------------- Details Scraping --------------------
    def _initialize_details_csv(self, output_csv, append=False):
        with self.details_csv_lock:
            file_exists = os.path.isfile(output_csv) and append
            self.details_csv_file = open(output_csv, "a" if append else "w", newline="", encoding="utf-8")
            self.details_csv_writer = csv.DictWriter(self.details_csv_file, fieldnames=self.fieldnames)
            if not file_exists:
                self.details_csv_writer.writeheader()
            self.log(f"Initialized CSV writer for {output_csv}", "INFO")


    def _close_details_csv(self):
        with self.details_csv_lock:
            if self.details_csv_file:
                self.details_csv_file.close()
                self.details_csv_file = None
                self.details_csv_writer = None
                self.log("Details CSV file closed.", "INFO")

    def extract_listing_details(self, url):
        if self.stop_requested.is_set():
            self.log(f"Skipping {url} as shutdown is requested.", "DEBUG")
            return None
        
        driver = None
        try:
            driver = self.driver_pool.acquire() # Acquire the single driver
            
            if self.stop_requested.is_set():
                self.log(f"Shutdown requested immediately after acquiring driver for {url}.", "DEBUG")
                return None

            driver.get(url)
            wait = WebDriverWait(driver, 10)

            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))

            def safe_text(by, selector, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text.strip()
                except (TimeoutException, NoSuchElementException, WebDriverException):
                    return None

            data = {
                "listing_title": safe_text(By.XPATH, '//*[@id="detail_title"]'),
                "property_id": safe_text(By.CSS_SELECTOR, '#container-property div:nth-child(5) div.flex.cursor-pointer p'),
                "total_price": safe_text(By.XPATH, '//*[@id="total-price"]'),
                "unit_price": safe_text(By.XPATH, '//*[@id="unit-price"]'),
                "property_url": url,
                "alley_width": safe_text(By.XPATH, '//*[@id="overview_content"]//div[@data-impression-index="1"]'),
                "image_url": None,
                "city": None,
                "district": None,
                "features": [],
                "property_description": []
            }

            # Image URL
            try:
                img_el = driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = img_el.get_attribute("imagesrcset")
                if image_src:
                    data["image_url"] = image_src.split(',')[0].strip().split(' ')[0]
            except Exception:
                pass

            # Breadcrumbs
            try:
                script_elements = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
                for script_el in script_elements:
                    try:
                        json_data = json.loads(script_el.get_attribute("innerHTML"))
                        if isinstance(json_data, dict) and json_data.get("@type") == "BreadcrumbList":
                            for item in json_data.get("itemListElement", []):
                                if item.get("position") == 2:
                                    data["city"] = item.get("name")
                                elif item.get("position") == 3:
                                    data["district"] = item.get("name")
                            break
                    except Exception:
                        continue
            except Exception:
                pass

            # Features
            try:
                features = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="key-feature-item"]')))
                for ele in features:
                    try:
                        title_el = ele.find_element(By.XPATH, './/*[@id="item_title"]')
                        text_el = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]')
                        title = title_el.text.strip() if title_el else None
                        text = text_el.text.strip() if text_el else None
                        if title and text:
                            data["features"].append(f"{title}: {text}")
                    except NoSuchElementException:
                        continue
            except Exception:
                pass

            # Description
            try:
                desc_div = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="property-description"]')
                if desc_div and desc_div.text:
                    data["property_description"] = [desc_div.text.strip()]
                else:
                    desc_elements = wait.until(EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, 'ul[aria-label="description-heading"].relative li')
                    ))
                    data["property_description"] = [
                        li.text.strip() for li in desc_elements if li.text.strip()
                    ]
            except Exception:
                pass
            
            self.log(f"Successfully extracted details for {url}", "DEBUG")
            return data
        except (TimeoutException, NoSuchElementException) as e:
            self.log(f"Selenium timeout or element not found for {url}: {e}", "WARN")
            return None
        except WebDriverException as e:
            self.log(f"WebDriver error for {url}: {e}. Attempting to re-initialize driver.", "ERROR")
            try:
                if self.driver_pool.driver:
                    self.driver_pool.driver.quit()
                    self.driver_pool.driver = None 
            except Exception as ex:
                self.log(f"Error quitting driver after WebDriverException for {url}: {ex}", "ERROR")
            return None
        except RuntimeError as e:
            self.log(f"Scraping for {url} cancelled during driver acquisition: {e}", "INFO")
            return None
        except Exception as e:
            self.log(f"An unexpected error occurred while scraping {url}: {e}", "ERROR")
            return None
        finally:
            if driver:
                self.driver_pool.release(driver)


    def save_details_to_csv(self, listing):
        if self.details_csv_writer and listing:
            listing_copy = listing.copy()
            listing_copy["features"] = "; ".join(listing_copy.get("features", []))
            listing_copy["property_description"] = ". ".join(listing_copy.get("property_description", []))
            
            filtered_listing = {k: v for k, v in listing_copy.items() if k in self.fieldnames}

            with self.details_csv_lock:
                try:
                    self.details_csv_writer.writerow(filtered_listing)
                    self.details_csv_file.flush()
                except Exception as e:
                    self.log(f"Error writing to CSV: {e} with data: {filtered_listing}", "ERROR")

    def scrape_with_retries(self, url):
        if self.stop_requested.is_set():
            self.log(f"Stopping retries for {url} as shutdown is requested.", "DEBUG")
            return None
        
        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_requested.is_set():
                self.log(f"Stopping retries for {url} in attempt {attempt} as shutdown is requested.", "DEBUG")
                return None
            try:
                result = self.extract_listing_details(url)
                if result is not None:
                    return result
            except RuntimeError:
                self.log(f"Scraping for {url} cancelled during retry attempt {attempt} due to shutdown.", "INFO")
                return None
            except Exception as e:
                self.log(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}", "WARN")
                if attempt < MAX_RETRIES and not self.stop_requested.is_set():
                    time.sleep(RETRY_DELAY)
        self.log(f"All {MAX_RETRIES} attempts failed for {url}.", "ERROR")
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
        
        if not urls:
            self.log("No URLs found in JSON file to process.", "INFO")
            return

        # Load processed property_urls from CSV
        processed_urls = set()
        if os.path.exists(output_csv):
            with open(output_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if "property_url" in row and row["property_url"]:
                        processed_urls.add(row["property_url"])

        self.log(f"Found {len(processed_urls)} already processed listings in {output_csv}", "INFO")

        self._initialize_details_csv(output_csv, append=True)

        urls_to_process = list(set(urls) - processed_urls)

        if not urls_to_process:
            self.log("All listings already scraped. Nothing to do.", "INFO")
            return

        self.log(f"Starting to scrape {len(urls_to_process)} listings...", "INFO")
        
        try:
            for url in tqdm(urls_to_process, desc="Scraping listing details"):
                if self.stop_requested.is_set():
                    self.log("Shutdown requested. Stopping further detail scraping.", "INFO")
                    break

                result = self.scrape_with_retries(url)
                if result:
                    self.save_details_to_csv(result)
        except KeyboardInterrupt:
            self.log("KeyboardInterrupt detected. Initiating graceful shutdown...", "INFO")
            self.stop_requested.set()
        except Exception as e:
            self.log(f"An unexpected error occurred during sequential scraping: {e}", "CRITICAL")
            self.stop_requested.set()
        finally:
            self.shutdown()


    def shutdown(self):
        self.log("Shutting down scraper components...", "INFO")
        self.stop_requested.set()
        self.driver_pool.close_all()
        self._close_details_csv()
        self.log("Scraper shutdown complete.", "INFO")