import logging
import os
import json
from datetime import datetime, timezone
import supabase
from supabase import create_client, Client
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from time import sleep
from random import uniform
import requests
import tqdm
import re
import pandas as pd
from urllib.parse import unquote

def get_mubawab_external_id(url):
    """
    Extracts the Mubawab listing ID from the URL.
    Example URL: https://www.mubawab.ma/en/a/7336518/some-title
    Returns: '7336518' or None if not found.
    """
    match = re.search(r"/a/(\d+)", url)
    if match:
        return match.group(1)
    return None

def save_raw_listing(source: str, link: str, response_text: str) -> None:
    """
    Inserts or upserts the raw listing into the raw_listings table.
    """
    external_id = get_mubawab_external_id(link)

    if external_id is None:
        logging.warning(f"Could not extract external_id from URL: {link}")
        return

    payload = {
        "url": link,
        "html": response_text,
        "scraped_at_client": datetime.now(timezone.utc).isoformat()
    }

    try:
        supabase.table("raw_listings").upsert(
            {
                "source": source,
                "external_id": external_id,
                "payload_json": payload,
            },
            on_conflict=["source", "external_id"],
        ).execute()
    except Exception as e:
        logging.error(f"Error saving raw listing to Supabase for {link}: {e}")

def get_links(url, max_pages=1):
    """
    Scrapes property links from mubawab.ma and handles pagination.

    Args:
        url (str): The base URL containing all the property listings.
        max_pages (int, optional): Number of pages to scrape. Defaults to 20.

    Returns:
        list: URLs of all the specific property pages to be scraped.
    """
    prop_links = []
    page = 1

    while page <= max_pages:
        page_url = f"{url}:p:{page}"
        
        try:
            response = requests.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            listings = soup.find_all('div', class_="listingBox sPremium")
            if not listings:
                logging.info(f"No listings found on page {page}. Stopping pagination.")
                break

            for listing in listings:
                try:
                    link = listing.get('linkref')

                    if not link:
                        continue

                    prop_links.append((link))

                except Exception as e:
                    logging.error(f"Error parsing listing: {e}")

            page += 1
            sleep(uniform(1, 3))

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error on page {page}: {http_err}")
            break
        except requests.RequestException as req_err:
            logging.error(f"Request error on page {page}: {req_err}")
            break

    return prop_links

def get_details(links):
    """
    Scrapes the important features of each property.

    Args:
        links (str): The URLs of each property to be scraped.

    Returns:
        DataFrame: A pandas DataFrame containing all the features of the 
        property
    """
    full_list = []
    
    for link in tqdm.tqdm(links, desc="Scraping property details"):
        try:
            response = requests.get(link)
            response.raise_for_status()

            save_raw_listing(source="mubawab", link=link, response_text=response.text)

            soup = BeautifulSoup(response.content, 'html.parser')

            raw_price = soup.find('h3', class_='orangeTit').text.strip()
            raw_area_text = soup.find('h3', class_='greyTit').text.strip()
            raw_title = soup.find('h1', class_='searchTitle').text.strip()
            
            price = clean_integer(raw_price)
            title = clean_text(raw_title)
            
            area, city = parse_area_and_city(raw_area_text)
            
            text_content = None
            description_div = soup.find('div', class_='wordBreak')
            if not description_div:
                for p in soup.find_all('p'):
                    if len(p.text.strip()) > 50:
                        text_content = p.get_text(separator=" ").strip()
                        break
            else:
                text_content = description_div.get_text(separator=" ").strip()

            features_block = soup.find("div", class_="adFeatures")

            label_value = {}

            if features_block:
                for content in features_block.select("div.adMainFeatureContent"):
                    label_tag = content.find("p", class_="adMainFeatureContentLabel")
                    value_tag = content.find("p", class_="adMainFeatureContentValue")
                    if not label_tag or not value_tag:
                        continue

                    label = clean_att(label_tag.get_text())
                    value = clean_att(value_tag.get_text())
                    label_value[label] = value

            prop_type = label_value.get("Type of property")
            condition = label_value.get("Condition")
            age = label_value.get("Age")
            orientation = label_value.get("Orientation")
            flooring = label_value.get("Flooring")
            floor_number = label_value.get("Floor number")
            number_of_floors = label_value.get("Number of floors")
            
            size = rooms = bedrooms = bathrooms = None
            details = soup.find_all('div', class_='adDetailFeature')

            for detail in details:
                text = detail.text
                value = detail.find('span').text.strip()
                
                if 'm²' in text:
                    size = clean_integer(value)
                if 'Pieces' in text or 'Piece' in text:
                    rooms = clean_integer(value)
                if 'Rooms' in text or 'Room' in text:
                    bedrooms = clean_integer(value)
                if 'Bathrooms' in text or 'Bathroom' in text:
                    bathrooms = clean_integer(value)
                    
            if rooms is None and text_content:
                rooms = clean_rooms(text_content)
                    
            features = soup.find_all('p', class_='fSize11 centered')
            feature_list = [clean_text(feature.text) for feature in features]   
            feature_str = ', '.join(filter(None, feature_list))
            
            lat, lon = extract_coordinates(soup)
                     
            property_details = {
                'title': title,
                'description': text_content,
                'property_type': prop_type,
                'city': city, 
                'area': area, 
                'size': size, 
                'rooms': rooms, 
                'bedrooms': bedrooms, 
                'bathrooms': bathrooms, 
                'price': price,
                'features': feature_str,
                'condition': condition,
                'age': clean_age(str(age)),
                'Orientation': orientation,
                'flooring': flooring,
                'floor_number': floor_number,
                'number_of_floors': number_of_floors,
                'lat': lat,
                'lon': lon,
                'url': link
            }
            
            full_list.append(property_details)
            sleep(uniform(1, 3))
            
        except Exception as e:
            logging.error(f'Error fetching property data from {link}: {e}')
            
    return pd.DataFrame(full_list)


def clean_integer(number_str):
    """
    Cleans all numerate fields by removing any non-digit characters, and 
    converting it into an integer value.

    Args:
        number_str (str): The price string of the property.

    Returns:
        int or None: The cleaned price as an integer, or None if invalid.
    """
    
    if not number_str:
        return None
    try:
        # Remove all non-digit characters
        number_str = re.sub(r'[^\d]', '', number_str)
        return int(number_str)
    except ValueError:
        return None
    
def clean_text(text):
    return text.strip() if text else None

def clean_att(s: str) -> str:
    return " ".join(s.split()).strip()

def clean_age(age_str):
    """
    Cleans the age string by extracting age ranges in the format 'min-max' 
    only if 'years' is in the original string and exactly two numbers are 
    present.

    Args:
        age_str (str): The age string to clean.

    Returns:
        str or None: The age range in 'min-max' format, or None if conditions 
        are not met.
    """
    if not age_str:
        return None

    # Convert the string to lowercase for case-insensitive matching
    age_str_lower = age_str.lower()

    # Check if 'years' is in the original string
    if 'years' not in age_str_lower:
        return None

    # Extract all numbers from the string
    numbers = re.findall(r'\d+', age_str)
    if len(numbers) == 2:
        # Exactly two numbers found, format as 'min-max'
        min_age = int(numbers[0])
        max_age = int(numbers[1])
        return f"{min_age}-{max_age}"
    else:
        # Either less than or more than two numbers found
        return None
        
def clean_rooms(description):
    pattern = r'(\d+)\s*(?:\w+\s)?rooms?\b'
    match = re.search(pattern, description, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None

def clean_condition(cond_str):
    try:
        if not cond_str:
            return None
        
        if cond_str == 'Good condition':
            cond_str = 'Good'
            return cond_str
        elif cond_str == 'Due for reform':
            cond_str == 'Old'
            return cond_str
        elif cond_str == 'New':
            return cond_str
        else:
            return None
    except ValueError:
        return None
    
def parse_area_and_city(raw_area_text):
    if not raw_area_text:
        logging.warning("raw_area_text is None or empty.")
        return None, None
    raw_area_text = raw_area_text.strip()
    pattern = r'^(.*)\s+in\s+(.*)$'
    match = re.search(pattern, raw_area_text, re.IGNORECASE)
    if match:
        area = match.group(1).strip()
        city = match.group(2).strip()
        logging.debug(f"Parsed area: '{area}', city: '{city}' from raw_area_text: '{raw_area_text}'")
    else:
        area = None
        city = raw_area_text.strip()
        logging.debug(f"No 'in' found. Set area to None and city to '{city}' from raw_area_text: '{raw_area_text}'")
    return area, city

def extract_coordinates(soup):
    scripts = soup.find_all("script")
    for s in scripts:
        if s.string and "waze.com/ul" in s.string:
            match = re.search(r"waze\.com/ul\?ll=([^&]+)", s.string)
            if match:
                ll = unquote(match.group(1))
                lat, lon = ll.split(",")
                lat = float(lat)
                lon = float(lon)
    
    return lat, lon