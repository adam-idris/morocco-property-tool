import logging
from scraper import get_links, get_details

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # 1. Choose the base URL and number of pages to scrape
    base_url = "https://www.mubawab.ma/en/cc/real-estate-for-sale"
    max_pages = 1 

    logging.info("Starting link scraping...")
    links = get_links(base_url, max_pages=max_pages)
    logging.info(f"Found {len(links)} property links.")

    # 2. Scrape each property, upload raws to Supabase, and get cleaned DataFrame
    logging.info("Scraping property details and uploading raw listings...")
    df = get_details(links)

    # 3. (Optional) save the cleaned data locally for now
    # output_path = "mubawab_cleaned.pkl"
    # df.to_pickle(output_path)
    # logging.info(f"Saved cleaned data to {output_path}")
    # logging.info("Done.")


if __name__ == "__main__":
    main()
