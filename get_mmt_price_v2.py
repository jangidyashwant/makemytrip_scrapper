import json
import time
import os
import re
import random, copy
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import requests
import yaml
import logging
import argparse
import zipfile
from random import randint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from lxml import etree

# Constants
MAX_RETRIES = 4
POOL_SIZE = 3
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DRIVER_PATH = os.path.join(BASE_DIR, "DRIVER", "chromedriver")
PROXY_FILE = os.path.join(BASE_DIR, "Webshare residential proxies.txt")
CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")
payload = {'deviceDetails': {'appVersion': '134.0.0.0', 'deviceId': '5a0d9450-9716-486c-9947-e27375ca3b2b', 'deviceType': 'Desktop', 'bookingDevice': 'DESKTOP', 'deviceName': None}, 'searchCriteria': {'hotelId': '202304261920025912', 'checkIn': '2025-05-06', 'checkOut': '2025-05-14', 'roomStayCandidates': [{'rooms': 1, 'adultCount': 2, 'childAges': []}], 'comparatorHotelIds': [], 'countryCode': 'IN', 'cityCode': 'CTGGN', 'locationId': 'RGNCR', 'locationType': 'region', 'currency': 'INR', 'limit': 20, 'personalCorpBooking': False, 'userSearchType': 'hotel'}, 'requestDetails': {'visitorId': '97adc924-f093-4cec-8107-c9965a6ea87a', 'visitNumber': 1, 'trafficSource': None, 'loggedIn': False, 'funnelSource': 'HOTELS', 'idContext': 'B2C', 'notifCoupon': None, 'pageContext': 'DETAIL', 'channel': 'B2Cweb', 'seoCorp': False, 'payMode': None, 'isExtendedPackageCall': False, 'forwardBookingFlow': False}, 'featureFlags': {'addOnRequired': True, 'applyAbsorption': True, 'bestCoupon': True, 'freeCancellationAvail': True, 'responseFilterFlags': True, 'soldOutInfoReq': True, 'walletRequired': True, 'bestOffersLimit': 3}, 'filterCriteria': [], 'expData': '{APE:10,PAH:5,PAH5:T,WPAH:F,BNPL:T,MRS:T,PDO:PN,MCUR:T,ADDON:T,CHPC:T,AARI:T,NLP:Y,RCPN:T,PLRS:T,MMRVER:V3,BLACK:T,IAO:F,BNPL0:T,EMIDT:1,CRI:T,GBE:T,CV2:T,MLOS:T,SOU:T,APT:T,SRRP:T,AIP:T,PLV2:T,RTBC:T,SPKG:T,TFT:T,GALLERYV2:T,NDD:T,UGCV2:T}'}
DATA_FOLDER = os.path.join(BASE_DIR, 'DATA')
if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
# MongoDB Configuration
MONGO_URI = "mongodb+srv://yash:Xcode#217@cluster0.hgmtv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "mmt_database"
COLLECTION_NAME = "mmt_rooms"

def setup_logging():
    log_folder = "LOGS"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    
    log_filename = os.path.join(log_folder, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    return log_filename

def connect_to_mongodb():
    try:
        client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        client.admin.command('ping')
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info("Connected to MongoDB successfully!")
        return collection
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None
    
def load_and_choose_random_proxy(file_path):
    """Reads proxies from a file and returns a randomly chosen (host, port, user, pass)."""
    proxy_list = []
    
    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split(":")
            if len(parts) == 4:  # Ensure valid proxy format
                host, port, user, pwd = parts
                proxy_list.append((host, port, user, pwd))
    
    if not proxy_list:
        raise ValueError("No valid proxies found in the file.")

    return random.choice(proxy_list)


def get_driver():
    proxy_host, proxy_port, proxy_user, proxy_pass = load_and_choose_random_proxy(PROXY_FILE)
    logging.info(f"Using Proxy: {proxy_host}:{proxy_port}")
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    proxy_plugin = create_proxy_extension(proxy_host, proxy_port, proxy_user, proxy_pass)
    chrome_options.add_extension(proxy_plugin)
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # chrome_options.add_experimental_option("prefs", prefs)
    # chrome_options.add_argument("--window-size=1728,1117")
    service = Service(DRIVER_PATH)

    return webdriver.Chrome(service=service, options=chrome_options)

def get_url_params(url):
    parsed_url = urlparse(url)
    params = parse_qs(parsed_url.query)
    
    return {key: value[0] if len(value) == 1 else value for key, value in params.items()}


def scroll_page(driver, times=10):
    for _ in range(times):
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
        time.sleep(randint(2,4 ))


def extract_hotels(driver, config):
    hotels = []
    logging.info(f"parsing listing data")
    listing_configs = config['mmt']['listing']
    try:
        tree = etree.HTML(driver.page_source)
        hotel_parser = tree.xpath(listing_configs['block'])

        for hotel in hotel_parser:
            try:
                parsed_dict = {}
                for l_parser in listing_configs:
                    try:
                        parsed_dict[l_parser] = hotel.xpath(listing_configs[l_parser])[0]
                    except:
                        parsed_dict[l_parser] = ""

                hotels.append(parsed_dict)
            except Exception as e:
                logging.error("Error extracting hotel details:", e)

    except Exception as e:
        logging.error(f"Error extracting hotel listing data: {str(e)}")
    logging.info(f"{len(hotels)} hotels found")
    return hotels

def get_status_code(driver):
    try:
        return driver.execute_script("return window.performance.getEntries()[0].responseStatus")
    except Exception:
        return None

def save_cookies(driver):
    return {cookie["name"]: cookie["value"] for cookie in driver.get_cookies()}

def get_correlation_key(pagesource):
    cor_key = ""
    try:
        cor_key = re.findall('correlationKey":"(.*?)"' , pagesource , re.DOTALL)[0]
    except Exception as e:
        logging.error(f"Error while fetching correlationKey : {str(e)}")
        

    return cor_key

def load_random_proxy():
    proxies = []
    
    with open(PROXY_FILE, "r") as f:
        for line in f:
            parts = line.strip().split(":")
            if len(parts) == 4:  
                host, port, user, pwd = parts
                proxy_url = f"http://{user}:{pwd}@{host}:{port}"
                proxies.append({"http": proxy_url, "https": proxy_url})
    
    if not proxies:
        raise ValueError("No valid proxies found in the file.")

    return random.choice(proxies)

def load_config_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

def convert_date_format(date_str):
    date_obj = datetime.strptime(date_str, "%d%m%Y")
    return date_obj.strftime("%Y-%d-%m")

def create_proxy_extension(proxy_host, proxy_port, proxy_user, proxy_pass):
    manifest_json = """{
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": ["proxy", "tabs", "unlimitedStorage", "storage", "<all_urls>", "webRequest", "webRequestBlocking"],
        "background": {"scripts": ["background.js"]},
        "minimum_chrome_version":"22.0.0"
    }"""
    background_js = f"""
    var config = {{mode: "fixed_servers", rules: {{singleProxy: {{scheme: "http", host: "{proxy_host}", port: parseInt({proxy_port})}}, bypassList: ["localhost"]}}}};
    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});
    chrome.webRequest.onAuthRequired.addListener(function(details) {{return {{authCredentials: {{username: "{proxy_user}", password: "{proxy_pass}"}}}}}}, {{urls: ["<all_urls>"]}}, ["blocking"]);
    """
    plugin_file = "proxy_auth_plugin.zip"
    with zipfile.ZipFile(plugin_file, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)
    return plugin_file

def get_url_resp(product_api, headers, cookies, payload):
    max_retries = 3
    
    for attempt in range(max_retries):
        proxies = load_random_proxy()  # Pick a new proxy
        logging.info(f"Using Proxy: {proxies['http']}")

        try:
            resp = requests.post(product_api, json=payload, cookies=cookies, headers=headers, proxies=proxies, timeout=20)
            
            if resp.status_code == 200:
                return resp
            else:
                logging.info(f"Attempt {attempt + 1} failed: Status {resp.status_code}. Retrying with new proxy...")

        except requests.exceptions.RequestException as e:
            logging.info(f"Attempt {attempt + 1} failed: {e}")

        if attempt < max_retries - 1:
            time.sleep(randint(1, 4))
        else:
            logging.info("Max retries reached. Request failed.")
            return None    
        
def scrape_hotels(url, config, max_retries=MAX_RETRIES ):
    for attempt in range(max_retries):
        try:
            driver = get_driver()
            driver.get("https://www.makemytrip.com")
            time.sleep(randint(0, 4))
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, config['mmt']['listing']['block']))
                )        
            except TimeoutException:
                logging.info(f"Retry {attempt + 1}/{max_retries}: Listing block not found. Retrying...")
                driver.quit()
                time.sleep(2)
                continue     
            scroll_page(driver)
            time.sleep(randint(2, 4))
            status_code = get_status_code(driver)
            
            if status_code == 200:
                correlationKey = get_correlation_key(driver.page_source)
                hotels = extract_hotels(driver, config)
                cookies_dict = save_cookies(driver)
                driver.quit()
                return hotels, cookies_dict, status_code, correlationKey
            else:
                logging.info(f"Retry {attempt + 1}/{max_retries}: Page status {status_code}. Retrying...")
                driver.quit()
                time.sleep(2) 

        except (TimeoutException, WebDriverException, TimeoutError) as e:
            logging.error(f"Retry {attempt + 1}/{max_retries}: Encountered {type(e).__name__}: {e}. Retrying...")
        
        finally:
            if driver:
                driver.quit() 

    logging.info("Max retries reached. Failed to get valid listing page.")
    return [], {}, None

def fetch_api_data(product_api,headers, cookies, payload, outfile_name, collection ):
    resp = get_url_resp(product_api,headers, cookies, payload)
    if resp.status_code == 200:
        try:
            for data in resp.json().get("response",{}).get("exactRooms",[]):
                with open(outfile_name, "a") as outfile:
                    outfile.write(json.dumps(data)+"\n")
                
                try:
                    collection.insert_one(data) 
                except json.JSONDecodeError as json_error:
                    logging.info(f"Skipping invalid JSON line- Error: {json_error}")
                time.sleep(randint(1, 4))
        except Exception as e:
            logging.error("Issue while saving the data to file and mongo")

def main():
    parser = argparse.ArgumentParser(description="Scrape MakeMyTrip hotel listings using Selenium with rotating proxy and retries")
    parser.add_argument("--url", required=True, help="MakeMyTrip hotel listing URL")
    args = parser.parse_args()
    log_file = setup_logging()
    logging.info("Starting MakeMyTrip Scraper")
    collection = connect_to_mongodb()

    config_file_path = os.path.join(BASE_DIR,"config.yaml")
    config = load_config_from_yaml(config_file_path)
    hotels, cookies, status_code, requestId = scrape_hotels(args.url, config)
    prod_config = config['mmt']['product']
    product_api = prod_config['product_api']
    dvid = cookies.get("dvid","")
    vid = cookies.get("mcid","")
    # payload = prod_config['payload']
    headers = prod_config['headers']
    headers['visitor-id'] = vid
    payload['deviceDetails']['deviceId'] = dvid
    payload['requestDetails']['visitorId'] = vid
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S") 
    
    outfile_name = os.path.join(DATA_FOLDER, f"mmt_search_data_{timestamp}.json")
    hotel_urls = [h['hotel_url'] for h in hotels]
    
    if status_code == 200:
        for hotel_url in hotel_urls:

            url_params = get_url_params(hotel_url)
            payload['searchCriteria']['hotelId'] = url_params['hotelId']
            payload['searchCriteria']['checkIn'] =    datetime.strptime(url_params['checkin'], "%m%d%Y").strftime("%Y-%m-%d")
            payload['searchCriteria']['checkOut'] = datetime.strptime(url_params['checkout'], "%m%d%Y").strftime("%Y-%m-%d")
            logging.info(f"Fetching Room {url_params['hotelId']} details using POST API")

            fetch_api_data(product_api,headers, cookies, payload, outfile_name, collection) 
           
            
            
            # resp = get_url_resp(product_api,headers, cookies, payload)
            # if resp.status_code == 200:
            #     try:
            #         for data in resp.json().get("response",{}).get("exactRooms",[]):
            #             outfile.write(json.dumps(data)+"\n")
                        
            #             try:
            #                 collection.insert_one(data) 
            #             except json.JSONDecodeError as json_error:
            #                 logging.info(f"Skipping invalid JSON line- Error: {json_error}")
            #             time.sleep(randint(1, 4))
            #     except Exception as e:
            #         logging.error("Issue while saving the data to file and mongo")
        

    logging.info("Scraping completed. Data saved.")


if __name__ == "__main__":
    main()
