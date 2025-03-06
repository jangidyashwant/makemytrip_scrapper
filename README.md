# MakeMyTrip Scraper

This scraper takes a **search URL** as an argument, opens the URL using **Selenium**, fetches **hotel listing URLs**, and saves cookies from the page to use in **search-rooms API requests** for each hotel. The extracted data is stored in a **JSON file** and optionally pushed to a **MongoDB server**.

## Prerequisites
Before running the scraper, ensure you have the following installed:

- Python 3.x
- Google Chrome browser
- ChromeDriver (place the `chromedriver` in the `driver` folder)
- MongoDB 

### Install Required Python Libraries
Run the following command to install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Scraper
Use the following command to run the scraper:
```bash
python3 get_mmt_price_v2.py --url "https://www.makemytrip.com/hotels/hotel-listing/?checkin=04062025&city=RGNCR&checkout=04142025&roomStayQualifier=2e0e&locusId=RGNCR&country=IN&locusType=region&searchText=New+Delhi+and+NCR&regionNearByExp=3&rsc=1e2e0e"
```

## Features
- **Extracts hotel listing URLs** from the search results page
- **Saves cookies** for API authentication
- **Rotate proxies** when the URL or API fails and try with new session
- **Fetches search-rooms API data** for each hotel
- **Stores extracted data in JSON**
- **Also pushes data to MongoDB**


## Output
The scraper saves extracted data in a JSON file and, if enabled, in MongoDB for further analysis.


