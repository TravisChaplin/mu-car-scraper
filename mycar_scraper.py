import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import extras # Import extras for execute_values
import re
import time
from datetime import datetime, UTC
import os

BASE_URL = "https://www.mycar.mu/car/buy"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

# Retrieve Supabase connection details from environment variables
SUPABASE_HOST = os.environ.get("SUPABASE_HOST")
SUPABASE_DATABASE = os.environ.get("SUPABASE_DATABASE")
SUPABASE_USER = os.environ.get("SUPABASE_USER")
SUPABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD")
SUPABASE_PORT = os.environ.get("SUPABASE_PORT", "5432")

def init_db():
    """
    Establishes a connection to the Supabase PostgreSQL database and creates
    the 'listings' table if it doesn't exist, with the specified schema.
    Returns the database connection object.
    """
    try:
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DATABASE,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD,
            port=SUPABASE_PORT
        )
        cur = conn.cursor()

        # Create the listings table in PostgreSQL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id SERIAL PRIMARY KEY,
                listing_id TEXT UNIQUE,
                make TEXT,
                model TEXT,
                year INTEGER,
                mileage_km INTEGER,
                transmission TEXT,
                fuel_type TEXT,
                price_rs INTEGER,
                price_rating TEXT,
                engine_capacity TEXT,
                body_type TEXT,
                new_pre_owned TEXT,
                duty TEXT,
                url TEXT,
                scraped_at TIMESTAMP WITH TIME ZONE
            );
        "")
        # Create index for faster queries
        cur.execute("CREATE INDEX IF NOT EXISTS idx_make_model_year ON listings(make, model, year);")
        conn.commit()
        print("Connected to Supabase and 'listings' table ensured.")
        return conn
    except Exception as e:
        print(f"Error connecting to Supabase or creating table: {e}")
        return None

def parse_card(card):
    try:
        title_link = card.find("a", class_="title", href=re.compile(r"/car/buy/\\d+"))
        if not title_link:
            return None

        url = title_link["href"]
        lid = url.split("/")[-1]

        make = None
        model = None
        engine_capacity = None
        body_type_from_title_span = None

        title_span = title_link.find("span", itemprop="name")
        if title_span:
            main_title_text = title_span.find(string=True, recursive=False).strip() if title_span.find(string=True, recursive=False) else ""
            parts = main_title_text.split(maxsplit=1)
            make = parts[0] if parts else None
            model = " ".join(parts[1:]) if len(parts) > 1 else None

            engine_span = title_span.find("span", class_="font-weight-lighter")
            if engine_span:
                full_engine_info_text = engine_span.get_text(strip=True)
                engine_match = re.search(r"(\\d+(?:\\.\\d+)?(?:\\s*cc)?)\\s*(.*)", full_engine_info_text, re.IGNORECASE)
                if engine_match:
                    engine_capacity = engine_match.group(1).strip()
                    remaining_text = engine_match.group(2).strip()
                    if remaining_text:
                        body_type_from_title_span = remaining_text
                else:
                    body_type_from_title_span = full_engine_info_text

                if model and full_engine_info_text and model.endswith(full_engine_info_text):
                    model = model[:-len(full_engine_info_text)].strip()

        year_span = card.find("span", class_=re.compile(r"\\w{3}-\\d{4}"))
        year = int(year_span.text.split()[-1]) if year_span else None

        mileage_span = card.find("span", itemprop="value")
        mileage_km = int(re.sub(r"[^\\d]", "", mileage_span.text)) if mileage_span else None

        transmission_span = card.find("span", itemprop="vehicleTransmission")
        transmission = transmission_span.text.strip() if transmission_span else None

        fuel_type_span = card.find("span", itemprop="fuelType")
        fuel_type = fuel_type_span.text.strip() if fuel_type_span else None

        price_rs = None
        price_span = card.find("span", class_="price")
        if price_span:
            price_text = price_span.get_text(strip=True)
            match = re.search(r"Rs\\s*([\\d,]+)(?:\\*)?", price_text)
            if match:
                price_rs = int(match.group(1).replace(",", ""))

        rating_div = card.find("div", class_=re.compile(r"mcc-ptag"))
        rating = rating_div.text.strip() if rating_div else None

        body_type = body_type_from_title_span
        new_pre_owned = None
        duty = None

        detail_soup = None
        if not body_type or not new_pre_owned or not duty:
            try:
                r_detail = requests.get(url, headers=HEADERS, timeout=10)
                r_detail.raise_for_status()
                detail_soup = BeautifulSoup(r_detail.text, "html.parser")
                time.sleep(0.5)
            except Exception as e:
                print(f"Error fetching detail page {url}: {e}")

        if detail_soup:
            if not new_pre_owned:
                description_meta = detail_soup.find('meta', attrs={'name': 'description'})
                if description_meta and 'content' in description_meta.attrs:
                    desc_text = description_meta['content']
                    if "Used" in desc_text:
                        new_pre_owned = "Used"
                    elif "New" in desc_text:
                        new_pre_owned = "New"

            if not body_type:
                description_meta = detail_soup.find('meta', attrs={'name': 'description'})
                if description_meta and 'content' in description_meta.attrs:
                    desc_text = description_meta['content']
                    common_body_types = ['suv', 'sedan', 'hatchback', 'coupe', 'pickup', 'wagon', 'convertible', 'minivan', 'crossover', 'van']
                    for bt_keyword in common_body_types:
                        if bt_keyword in desc_text.lower():
                            body_type = bt_keyword
                            break

            if not duty:
                duty_paid_elem = detail_soup.find(lambda tag: tag.name in ['span', 'div', 'li'] and 'Duty-paid' in tag.get_text(strip=True))
                if duty_paid_elem:
                    duty = "Duty Paid"
                else:
                    duty_free_elem = detail_soup.find(lambda tag: tag.name in ['span', 'div', 'li'] and 'Duty-free' in tag.get_text(strip=True))
                    if duty_free_elem:
                        duty = "Duty Free"


        return dict(
            listing_id=lid,
            make=make,
            model=model,
            year=year,
            mileage_km=mileage_km,
            transmission=transmission,
            fuel_type=fuel_type,
            price_rs=price_rs,
            price_rating=rating,
            engine_capacity=engine_capacity,
            body_type=body_type,
            new_pre_owned=new_pre_owned,
            duty=duty,
            url=url,
            scraped_at=datetime.now(UTC).isoformat()
        )
    except Exception as e:
        # print(f"Error parsing card: {e} in card: {card.prettify()[:500]}")
        return None

def get_total_pages():
    """Determines the total number of pages from the pagination links."""
    r = requests.get(BASE_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    pagination_links = soup.find_all("a", class_="page-link", href=re.compile(r"\\?page=\\d+"))
    if not pagination_links:
        return 1 # Assume only one page if no pagination links are found

    max_page = 0
    for link in pagination_links:
        try:
            page_num = int(link["href"].split("=")[-1])
            if page_num > max_page:
                max_page = page_num
        except (ValueError, KeyError):
            continue

    return max_page if max_page > 0 else 1

def scrape_page(page):
    url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    listings = []
    cards = soup.find_all(class_="offer-card")
    print(f"Found {len(cards)} cards on page {page}")
    for card in cards:
        rec = parse_card(card)
        if rec and rec["make"]:
            listings.append(rec)
    return listings

def save(conn, listings):
    """
    Saves a list of car listings to the Supabase PostgreSQL database.
    Uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) to handle existing records.
    """
    cur = conn.cursor()
    if not listings:
        print("No listings to save.")
        return 0

    columns = [
        "listing_id", "make", "model", "year", "mileage_km", "transmission",
        "fuel_type", "price_rs", "price_rating", "engine_capacity",
        "body_type", "new_pre_owned", "duty", "url", "scraped_at"
    ]

    # Prepare data for bulk insert
    values_to_insert = []
    for l in listings:
        row = []
        for col in columns:
            val = l.get(col)
            # Ensure scraped_at is in a format PostgreSQL expects (ISO 8601 string)
            if col == "scraped_at" and isinstance(val, str):
                row.append(val)
            elif col == "scraped_at" and val is None:
                row.append(datetime.now(UTC).isoformat()) # Default to current UTC time if not provided
            else:
                row.append(val)
        values_to_insert.append(tuple(row))

    # Construct the UPSERT statement
    insert_sql = f"""
        INSERT INTO listings ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (listing_id) DO UPDATE SET
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            year = EXCLUDED.year,
            mileage_km = EXCLUDED.mileage_km,
            transmission = EXCLUDED.transmission,
            fuel_type = EXCLUDED.fuel_type,
            price_rs = EXCLUDED.price_rs,
            price_rating = EXCLUDED.price_rating,
            engine_capacity = EXCLUDED.engine_capacity,
            body_type = EXCLUDED.body_type,
            new_pre_owned = EXCLUDED.new_pre_owned,
            duty = EXCLUDED.duty,
            url = EXCLUDED.url,
            scraped_at = EXCLUDED.scraped_at
    """

    try:
        # Use psycopg2.extras.execute_values for efficient bulk UPSERT
        extras.execute_values(cur, insert_sql, values_to_insert, page_size=100)
        conn.commit()
        return len(listings)
    except Exception as e:
        conn.rollback() # Rollback on error
        print(f"Error saving listings to Supabase: {e}")
        return 0

# --- Main execution block ---
print("Starting scraping and saving process to Supabase...")

supabase_conn = init_db()

if supabase_conn:
    total_pages = get_total_pages()
    print(f"Total pages to scrape: {total_pages}")

    for page in range(1, total_pages + 1):
        print(f"Scraping page {page}...")
        listings = scrape_page(page)
        n = save(supabase_conn, listings)
        print(f"Page {page}: {len(listings)} found, {n} saved/updated in Supabase")
        time.sleep(1.5)

    supabase_conn.close()
    print("Done! Data scraped and saved to Supabase.")
else:
    print("Supabase connection failed. Skipping scraping and saving.")
