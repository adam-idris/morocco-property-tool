#!/usr/bin/env python

import logging
import os
from dataclasses import asdict
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from supabase import create_client, Client
import tqdm

# ✨ Import your existing parsing + upsert logic from the main scraper
from scraper import (
    PropertyDetails,
    parse_property_page,
    upsert_normalised_listing,
)

# ---------- Supabase setup ----------

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Supabase URL or Key not found in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

SOURCE = "mubawab"
LISTING_TYPE = "rent"   # adjust if you later distinguish buy/rent in raw_listings
BATCH_SIZE = 200       # for paginating raw_listings

# ---------- Helpers ----------

def fetch_all_raw_listings(source: str = SOURCE) -> list[dict]:
    """
    Fetch ALL rows from raw_listings for a given source using paginated
    `.range()` queries (Supabase/PostgREST safe pagination).
    """
    all_rows: list[dict] = []
    start = 0

    while True:
        end = start + BATCH_SIZE - 1

        try:
            resp = (
                supabase.table("raw_listings")
                .select("external_id, payload_json")
                .eq("source", source)
                .order("external_id")
                .range(start, end)       # <-- POSTGREST-COMPLIANT PAGINATION
                .execute()
            )
        except Exception as e:
            logging.error(
                f"Failed to fetch batch start={start}, end={end}: {e}"
            )
            # If first batch fails, abort
            if start == 0:
                raise
            break

        batch = resp.data or []

        logging.info(
            f"Fetched batch {start}-{end}: {len(batch)} rows"
        )

        all_rows.extend(batch)

        # If batch smaller than BATCH_SIZE → last page reached
        if len(batch) < BATCH_SIZE:
            break

        start += BATCH_SIZE

    logging.info(
        f"TOTAL raw_listings loaded for source '{source}': {len(all_rows)}"
    )
    return all_rows


def wipe_normalised_listings(source: str = SOURCE) -> None:
    """
    Delete all rows in normalised_listings for the given source.
    This ensures we fully rebuild from raw_listings.
    """
    logging.info("Deleting existing normalised_listings rows for source='%s'...", source)
    (
        supabase.table("normalised_listings")
        .delete()
        .eq("source", source)
        .execute()
    )
    logging.info("Delete completed.")


# ---------- Core repair logic ----------

def rebuild_normalised_from_raw() -> None:
    """
    Main repair routine:
      1. Load all raw_listings for SOURCE.
      2. Wipe normalised_listings for SOURCE.
      3. For each raw row:
            - read url + html from payload_json
            - parse into PropertyDetails
            - upsert into normalised_listings
    """
    raw_rows = fetch_all_raw_listings(source=SOURCE)

    # Wipe old normalised rows so we rebuild everything from raw_listings
    wipe_normalised_listings(source=SOURCE)

    success_count = 0
    fail_count = 0
    parse_none_count = 0

    for row in tqdm.tqdm(raw_rows, desc="Rebuilding normalised_listings from raw_listings"):
        external_id = row.get("external_id")
        payload = row.get("payload_json") or {}

        url = payload.get("url")
        html = payload.get("html")

        if not external_id:
            logging.warning("Row without external_id, skipping: %s", row)
            fail_count += 1
            continue

        if not html or not url:
            logging.warning("Missing html/url in payload_json for external_id=%s, skipping.", external_id)
            fail_count += 1
            continue

        try:
            details = parse_property_page(url, html)
        except Exception as exc:
            logging.error(
                "Exception while parsing external_id=%s url=%s: %s",
                external_id,
                url,
                exc,
            )
            fail_count += 1
            continue

        if details is None:
            # parse_property_page decided this page is invalid/missing critical tags
            logging.warning(
                "parse_property_page returned None for external_id=%s url=%s",
                external_id,
                url,
            )
            parse_none_count += 1
            continue

        try:
            upsert_normalised_listing(
                details=details,
                external_id=external_id,
                source=SOURCE,
                listing_type=LISTING_TYPE,
                main_image_path=None,  # images are handled separately
            )
            success_count += 1
        except Exception as exc:
            logging.error(
                "Failed to upsert normalised_listing for external_id=%s url=%s: %s",
                external_id,
                url,
                exc,
            )
            fail_count += 1

    logging.info("=== Repair complete ===")
    logging.info("Total raw_listings processed: %d", len(raw_rows))
    logging.info("Successfully normalised:      %d", success_count)
    logging.info("parse_property_page == None: %d", parse_none_count)
    logging.info("Failed (exceptions/invalid): %d", fail_count)


# ---------- Entry point ----------

if __name__ == "__main__":
    rebuild_normalised_from_raw()