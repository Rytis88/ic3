#logic utils for handling common functions, a scraper module for scraping data from a website,
#    and a main script to orchestrate the scraping process.

from utils import save_to_file
from scraper import scrape_website

def main():
    url = "https://example.com"  # Change this to the URL you want to scrape
    data = scrape_website(url)
    if data:
        save_to_file(data, "scraped_data.html")

if __name__ == "__main__":
    main()