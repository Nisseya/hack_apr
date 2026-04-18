"""
SQL Schemas → Polars Benchmark Generator
==========================================
Generates synthetic data for Northwind, Chinook, Sakila, TPC-H, and Spider mini-DBs,
then creates selection-focused benchmark questions with gold eval.
"""

import polars as pl
import json, hashlib, random, os, re
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

random.seed(42)
OUTPUT_DIR = Path("polars_benchmark_sql")
DATASETS_DIR = OUTPUT_DIR / "datasets"
os.makedirs(DATASETS_DIR, exist_ok=True)

# ============================================================
# HELPERS
# ============================================================
def rand_date(start, end):
    d = start + timedelta(days=random.randint(0, (end - start).days))
    return d.strftime("%Y-%m-%d")

def rand_datetime(start, end):
    d = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    return d.strftime("%Y-%m-%d %H:%M:%S")

def rand_phone():
    return f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def rand_email(name):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "mail.com"]
    return f"{name.lower().replace(' ', '.')}@{random.choice(domains)}"

FIRST_NAMES = ["Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hank","Iris","Jack",
    "Karen","Leo","Mona","Nick","Olivia","Paul","Quinn","Rita","Sam","Tina",
    "Uma","Victor","Wendy","Xavier","Yara","Zack","Anna","Ben","Clara","David",
    "Elena","Felix","Gina","Hugo","Isla","James","Kira","Liam","Maya","Noah",
    "Omar","Petra","Rene","Sara","Tom","Vera","Will","Xena","Yves","Zoe"]
LAST_NAMES = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez",
    "Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor",
    "Moore","Jackson","Martin","Lee","Perez","Thompson","White","Harris","Sanchez",
    "Clark","Ramirez","Lewis","Robinson","Walker","Young","Allen","King","Wright","Scott",
    "Torres","Nguyen","Hill","Flores","Green","Adams","Nelson","Baker","Hall","Rivera"]

def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

COUNTRIES_EU = ["France","Germany","Spain","Italy","UK","Netherlands","Belgium","Austria","Sweden","Poland"]
CITIES_MAP = {
    "France": ["Paris","Lyon","Marseille","Toulouse","Nice","Bordeaux"],
    "Germany": ["Berlin","Munich","Hamburg","Frankfurt","Stuttgart","Cologne"],
    "Spain": ["Madrid","Barcelona","Valencia","Seville"],
    "Italy": ["Rome","Milan","Naples","Turin","Florence"],
    "UK": ["London","Manchester","Birmingham","Edinburgh","Bristol"],
    "Netherlands": ["Amsterdam","Rotterdam","The Hague","Utrecht"],
    "Belgium": ["Brussels","Antwerp","Ghent"],
    "Austria": ["Vienna","Salzburg","Graz"],
    "Sweden": ["Stockholm","Gothenburg","Malmö"],
    "Poland": ["Warsaw","Kraków","Gdańsk","Wrocław"],
}

def rand_country_city():
    c = random.choice(COUNTRIES_EU)
    return c, random.choice(CITIES_MAP[c])


# ============================================================
# 1. NORTHWIND DATA GENERATION
# ============================================================
def gen_northwind():
    tables = {}

    # Categories
    cat_names = ["Beverages","Condiments","Confections","Dairy Products","Grains/Cereals",
                 "Meat/Poultry","Produce","Seafood"]
    tables["nw_categories"] = pl.DataFrame({
        "category_id": list(range(1, len(cat_names)+1)),
        "category_name": cat_names,
        "description": [f"Description for {c}" for c in cat_names],
    })

    # Suppliers
    n_sup = 30
    sup_countries = [rand_country_city() for _ in range(n_sup)]
    tables["nw_suppliers"] = pl.DataFrame({
        "supplier_id": list(range(1, n_sup+1)),
        "company_name": [f"Supplier_{i} Co." for i in range(1, n_sup+1)],
        "contact_name": [rand_name() for _ in range(n_sup)],
        "contact_title": random.choices(["Sales Manager","Marketing Manager","Owner","Export Administrator","Sales Representative"], k=n_sup),
        "city": [c[1] for c in sup_countries],
        "country": [c[0] for c in sup_countries],
        "phone": [rand_phone() for _ in range(n_sup)],
    })

    # Products
    n_prod = 80
    tables["nw_products"] = pl.DataFrame({
        "product_id": list(range(1, n_prod+1)),
        "product_name": [f"Product_{i}" for i in range(1, n_prod+1)],
        "supplier_id": random.choices(range(1, n_sup+1), k=n_prod),
        "category_id": random.choices(range(1, len(cat_names)+1), k=n_prod),
        "unit_price": [round(random.uniform(2, 250), 2) for _ in range(n_prod)],
        "units_in_stock": [random.randint(0, 200) for _ in range(n_prod)],
        "units_on_order": [random.randint(0, 80) for _ in range(n_prod)],
        "reorder_level": [random.choice([0, 5, 10, 15, 20, 25, 30]) for _ in range(n_prod)],
        "discontinued": random.choices([True, False], weights=[0.1, 0.9], k=n_prod),
    })

    # Customers
    n_cust = 100
    cust_cc = [rand_country_city() for _ in range(n_cust)]
    cust_ids = [f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))}" for _ in range(n_cust)]
    tables["nw_customers"] = pl.DataFrame({
        "customer_id": cust_ids,
        "company_name": [f"Company_{i}" for i in range(1, n_cust+1)],
        "contact_name": [rand_name() for _ in range(n_cust)],
        "contact_title": random.choices(["Owner","Sales Associate","Marketing Manager","Accounting Manager","Sales Representative","Order Administrator"], k=n_cust),
        "city": [c[1] for c in cust_cc],
        "country": [c[0] for c in cust_cc],
        "phone": [rand_phone() for _ in range(n_cust)],
        "fax": [rand_phone() if random.random() > 0.4 else None for _ in range(n_cust)],
    })

    # Employees
    n_emp = 15
    titles = ["Sales Representative","Sales Manager","Vice President Sales","Inside Sales Coordinator"]
    tables["nw_employees"] = pl.DataFrame({
        "employee_id": list(range(1, n_emp+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_emp)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_emp)],
        "title": random.choices(titles, weights=[0.6, 0.2, 0.1, 0.1], k=n_emp),
        "birth_date": [rand_date(datetime(1955,1,1), datetime(1990,12,31)) for _ in range(n_emp)],
        "hire_date": [rand_date(datetime(2015,1,1), datetime(2023,12,31)) for _ in range(n_emp)],
        "city": random.choices(["London","Seattle","Tacoma","Redmond","Kirkland"], k=n_emp),
        "country": random.choices(["UK","USA"], weights=[0.3, 0.7], k=n_emp),
        "reports_to": [random.choice([None]+list(range(1, 5))) for _ in range(n_emp)],
    })

    # Shippers
    tables["nw_shippers"] = pl.DataFrame({
        "shipper_id": [1, 2, 3],
        "company_name": ["Speedy Express", "United Package", "Federal Shipping"],
        "phone": [rand_phone() for _ in range(3)],
    })

    # Regions / Territories
    tables["nw_regions"] = pl.DataFrame({
        "region_id": [1, 2, 3, 4],
        "region_description": ["Eastern", "Western", "Northern", "Southern"],
    })

    terr_names = ["New York","Boston","Philadelphia","Washington","Atlanta","Chicago",
                  "Detroit","Minneapolis","Denver","Seattle","San Francisco","Los Angeles",
                  "Houston","Dallas","London","Manchester","Edinburgh","Paris"]
    tables["nw_territories"] = pl.DataFrame({
        "territory_id": [f"T{str(i).zfill(3)}" for i in range(1, len(terr_names)+1)],
        "territory_description": terr_names,
        "region_id": random.choices([1,2,3,4], k=len(terr_names)),
    })

    # Orders
    n_orders = 1500
    tables["nw_orders"] = pl.DataFrame({
        "order_id": list(range(10248, 10248+n_orders)),
        "customer_id": random.choices(cust_ids, k=n_orders),
        "employee_id": random.choices(range(1, n_emp+1), k=n_orders),
        "order_date": [rand_date(datetime(2022,7,1), datetime(2024,12,31)) for _ in range(n_orders)],
        "required_date": [rand_date(datetime(2022,7,15), datetime(2025,1,15)) for _ in range(n_orders)],
        "shipped_date": [rand_date(datetime(2022,7,5), datetime(2025,1,5)) if random.random() > 0.05 else None for _ in range(n_orders)],
        "shipper_id": random.choices([1, 2, 3], k=n_orders),
        "freight": [round(random.uniform(1, 300), 2) for _ in range(n_orders)],
    })

    # Order Details
    details = []
    for oid in range(10248, 10248+n_orders):
        n_items = random.randint(1, 5)
        prods = random.sample(range(1, n_prod+1), min(n_items, n_prod))
        for pid in prods:
            details.append({
                "order_id": oid,
                "product_id": pid,
                "unit_price": round(random.uniform(5, 200), 2),
                "quantity": random.randint(1, 50),
                "discount": random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2, 0.25]),
            })
    tables["nw_order_details"] = pl.DataFrame(details)

    return tables


# ============================================================
# 2. CHINOOK DATA GENERATION
# ============================================================
def gen_chinook():
    tables = {}

    # Artists
    n_artists = 200
    tables["ck_artists"] = pl.DataFrame({
        "artist_id": list(range(1, n_artists+1)),
        "name": [f"Artist_{i}" for i in range(1, n_artists+1)],
    })

    # Albums
    n_albums = 350
    tables["ck_albums"] = pl.DataFrame({
        "album_id": list(range(1, n_albums+1)),
        "title": [f"Album_{i}" for i in range(1, n_albums+1)],
        "artist_id": random.choices(range(1, n_artists+1), k=n_albums),
    })

    # Media Types
    mt_names = ["MPEG audio file","Protected AAC audio file","Protected MPEG-4 video file","Purchased AAC audio file","AAC audio file"]
    tables["ck_media_types"] = pl.DataFrame({
        "media_type_id": list(range(1, len(mt_names)+1)),
        "name": mt_names,
    })

    # Genres
    genre_names = ["Rock","Jazz","Metal","Alternative","Classical","Blues","Latin","R&B/Soul",
                   "Reggae","Pop","Soundtrack","Hip Hop/Rap","Electronica","Country","Comedy","World"]
    tables["ck_genres"] = pl.DataFrame({
        "genre_id": list(range(1, len(genre_names)+1)),
        "name": genre_names,
    })

    # Tracks
    n_tracks = 2000
    tables["ck_tracks"] = pl.DataFrame({
        "track_id": list(range(1, n_tracks+1)),
        "name": [f"Track_{i}" for i in range(1, n_tracks+1)],
        "album_id": random.choices(range(1, n_albums+1), k=n_tracks),
        "media_type_id": random.choices(range(1, len(mt_names)+1), weights=[0.5,0.15,0.05,0.2,0.1], k=n_tracks),
        "genre_id": random.choices(range(1, len(genre_names)+1), k=n_tracks),
        "composer": [f"Composer_{random.randint(1,80)}" if random.random() > 0.2 else None for _ in range(n_tracks)],
        "milliseconds": [random.randint(60000, 600000) for _ in range(n_tracks)],
        "bytes": [random.randint(1000000, 50000000) for _ in range(n_tracks)],
        "unit_price": random.choices([0.99, 1.99], weights=[0.8, 0.2], k=n_tracks),
    })

    # Playlists
    pl_names = ["Music","Movies","TV Shows","Audiobooks","90s Music","Classical","Heavy Metal",
                "Brazilian Music","Grunge","On-The-Go","Jazz","Rock","Pop"]
    tables["ck_playlists"] = pl.DataFrame({
        "playlist_id": list(range(1, len(pl_names)+1)),
        "name": pl_names,
    })

    # Playlist Tracks
    pt_data = []
    for plid in range(1, len(pl_names)+1):
        n_tr = random.randint(10, 100)
        for tid in random.sample(range(1, n_tracks+1), min(n_tr, n_tracks)):
            pt_data.append({"playlist_id": plid, "track_id": tid})
    tables["ck_playlist_track"] = pl.DataFrame(pt_data)

    # Employees
    n_emp = 8
    titles_ck = ["General Manager","Sales Manager","IT Manager","Sales Support Agent","Sales Support Agent","Sales Support Agent","IT Staff","IT Staff"]
    tables["ck_employees"] = pl.DataFrame({
        "employee_id": list(range(1, n_emp+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_emp)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_emp)],
        "title": titles_ck[:n_emp],
        "reports_to": [None, 1, 1, 2, 2, 2, 3, 3],
        "birth_date": [rand_date(datetime(1960,1,1), datetime(1990,12,31)) for _ in range(n_emp)],
        "hire_date": [rand_date(datetime(2015,1,1), datetime(2023,6,30)) for _ in range(n_emp)],
        "city": random.choices(["Calgary","Edmonton","Lethbridge"], k=n_emp),
        "country": ["Canada"] * n_emp,
        "email": [f"emp{i}@chinookcorp.com" for i in range(1, n_emp+1)],
    })

    # Customers
    n_cust = 60
    cust_cc = [rand_country_city() for _ in range(n_cust)]
    tables["ck_customers"] = pl.DataFrame({
        "customer_id": list(range(1, n_cust+1)),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_cust)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_cust)],
        "company": [f"Company_{random.randint(1,30)}" if random.random() > 0.5 else None for _ in range(n_cust)],
        "email": [f"customer{i}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}" for i in range(1, n_cust+1)],
        "city": [c[1] for c in cust_cc],
        "state": [None if random.random() > 0.4 else f"State_{random.randint(1,20)}" for _ in range(n_cust)],
        "country": [c[0] for c in cust_cc],
        "support_rep_id": random.choices([4, 5, 6], k=n_cust),  # sales support agents
    })

    # Invoices
    n_inv = 500
    tables["ck_invoices"] = pl.DataFrame({
        "invoice_id": list(range(1, n_inv+1)),
        "customer_id": random.choices(range(1, n_cust+1), k=n_inv),
        "invoice_date": [rand_date(datetime(2021,1,1), datetime(2024,12,31)) for _ in range(n_inv)],
        "billing_city": [random.choice(CITIES_MAP[random.choice(COUNTRIES_EU)]) for _ in range(n_inv)],
        "billing_country": [random.choice(COUNTRIES_EU) for _ in range(n_inv)],
        "total": [round(random.uniform(0.99, 25.0), 2) for _ in range(n_inv)],
    })

    # Invoice Lines
    il_data = []
    il_id = 1
    for inv_id in range(1, n_inv+1):
        n_lines = random.randint(1, 8)
        for tid in random.sample(range(1, n_tracks+1), min(n_lines, n_tracks)):
            il_data.append({
                "invoice_line_id": il_id,
                "invoice_id": inv_id,
                "track_id": tid,
                "unit_price": random.choice([0.99, 1.99]),
                "quantity": 1,
            })
            il_id += 1
    tables["ck_invoice_lines"] = pl.DataFrame(il_data)

    return tables


# ============================================================
# 3. SAKILA DATA GENERATION
# ============================================================
def gen_sakila():
    tables = {}

    # Actors
    n_actors = 200
    tables["sk_actors"] = pl.DataFrame({
        "actor_id": list(range(1, n_actors+1)),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_actors)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_actors)],
    })

    # Categories
    cat_names = ["Action","Animation","Children","Classics","Comedy","Documentary",
                 "Drama","Family","Foreign","Games","Horror","Music","New","Sci-Fi","Sports","Travel"]
    tables["sk_categories"] = pl.DataFrame({
        "category_id": list(range(1, len(cat_names)+1)),
        "name": cat_names,
    })

    # Languages
    lang_names = ["English","Italian","Japanese","Mandarin","French","German"]
    tables["sk_languages"] = pl.DataFrame({
        "language_id": list(range(1, len(lang_names)+1)),
        "name": lang_names,
    })

    # Films
    n_films = 1000
    ratings = ["G","PG","PG-13","R","NC-17"]
    special_feat_options = ["Trailers","Commentaries","Deleted Scenes","Behind the Scenes"]
    tables["sk_films"] = pl.DataFrame({
        "film_id": list(range(1, n_films+1)),
        "title": [f"Film_{i}" for i in range(1, n_films+1)],
        "description": [f"A story about {random.choice(['adventure','love','mystery','action','comedy'])} in {random.choice(['a city','the jungle','space','a school','an island'])}" for _ in range(n_films)],
        "release_year": random.choices(range(2000, 2025), k=n_films),
        "language_id": random.choices(range(1, len(lang_names)+1), weights=[0.6,0.1,0.05,0.05,0.1,0.1], k=n_films),
        "rental_duration": random.choices([3, 4, 5, 6, 7], k=n_films),
        "rental_rate": random.choices([0.99, 2.99, 4.99], weights=[0.3, 0.5, 0.2], k=n_films),
        "length_min": [random.randint(46, 185) for _ in range(n_films)],
        "replacement_cost": [round(random.uniform(9.99, 29.99), 2) for _ in range(n_films)],
        "rating": random.choices(ratings, k=n_films),
        "special_features": [",".join(random.sample(special_feat_options, random.randint(1,3))) if random.random() > 0.1 else None for _ in range(n_films)],
    })

    # Film-Actor (M2M)
    fa_data = []
    for fid in range(1, n_films+1):
        for aid in random.sample(range(1, n_actors+1), random.randint(1, 8)):
            fa_data.append({"actor_id": aid, "film_id": fid})
    tables["sk_film_actor"] = pl.DataFrame(fa_data)

    # Film-Category (M2M)
    fc_data = []
    for fid in range(1, n_films+1):
        fc_data.append({"film_id": fid, "category_id": random.randint(1, len(cat_names))})
    tables["sk_film_category"] = pl.DataFrame(fc_data)

    # Countries, Cities, Addresses
    country_list = COUNTRIES_EU + ["USA","Canada","Brazil","Australia","Japan","India"]
    tables["sk_countries"] = pl.DataFrame({
        "country_id": list(range(1, len(country_list)+1)),
        "country": country_list,
    })

    all_cities_flat = []
    city_id = 1
    for cid, country in enumerate(country_list, 1):
        city_names = CITIES_MAP.get(country, [f"{country}_City_{i}" for i in range(1,4)])
        for cn in city_names:
            all_cities_flat.append({"city_id": city_id, "city": cn, "country_id": cid})
            city_id += 1
    tables["sk_cities"] = pl.DataFrame(all_cities_flat)
    n_cities = len(all_cities_flat)

    n_addr = 600
    tables["sk_addresses"] = pl.DataFrame({
        "address_id": list(range(1, n_addr+1)),
        "address": [f"{random.randint(1,999)} {random.choice(['Main St','Oak Ave','Park Blvd','Elm St','Maple Dr'])}" for _ in range(n_addr)],
        "district": [f"District_{random.randint(1,30)}" for _ in range(n_addr)],
        "city_id": random.choices(range(1, n_cities+1), k=n_addr),
        "postal_code": [f"{random.randint(10000,99999)}" if random.random() > 0.1 else None for _ in range(n_addr)],
        "phone": [rand_phone() for _ in range(n_addr)],
    })

    # Stores
    tables["sk_stores"] = pl.DataFrame({
        "store_id": [1, 2],
        "manager_staff_id": [1, 2],
        "address_id": [1, 2],
    })

    # Staff
    tables["sk_staff"] = pl.DataFrame({
        "staff_id": [1, 2, 3, 4],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(4)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(4)],
        "address_id": random.sample(range(1, 10), 4),
        "email": [f"staff{i}@sakilastore.com" for i in range(1, 5)],
        "store_id": [1, 2, 1, 2],
        "active": [True, True, True, False],
    })

    # Customers
    n_cust = 600
    tables["sk_customers"] = pl.DataFrame({
        "customer_id": list(range(1, n_cust+1)),
        "store_id": random.choices([1, 2], k=n_cust),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_cust)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_cust)],
        "email": [f"cust{i}@sakilamail.com" for i in range(1, n_cust+1)],
        "address_id": random.choices(range(1, n_addr+1), k=n_cust),
        "active": random.choices([True, False], weights=[0.95, 0.05], k=n_cust),
        "create_date": [rand_date(datetime(2020,1,1), datetime(2024,6,30)) for _ in range(n_cust)],
    })

    # Inventory
    n_inv = 4500
    tables["sk_inventory"] = pl.DataFrame({
        "inventory_id": list(range(1, n_inv+1)),
        "film_id": random.choices(range(1, n_films+1), k=n_inv),
        "store_id": random.choices([1, 2], k=n_inv),
    })

    # Rentals
    n_rent = 8000
    tables["sk_rentals"] = pl.DataFrame({
        "rental_id": list(range(1, n_rent+1)),
        "rental_date": [rand_datetime(datetime(2023,1,1), datetime(2024,12,31)) for _ in range(n_rent)],
        "inventory_id": random.choices(range(1, n_inv+1), k=n_rent),
        "customer_id": random.choices(range(1, n_cust+1), k=n_rent),
        "return_date": [rand_datetime(datetime(2023,1,3), datetime(2025,1,15)) if random.random() > 0.03 else None for _ in range(n_rent)],
        "staff_id": random.choices([1, 2, 3, 4], k=n_rent),
    })

    # Payments
    n_pay = 8000
    tables["sk_payments"] = pl.DataFrame({
        "payment_id": list(range(1, n_pay+1)),
        "customer_id": random.choices(range(1, n_cust+1), k=n_pay),
        "staff_id": random.choices([1, 2, 3, 4], k=n_pay),
        "rental_id": random.choices(range(1, n_rent+1), k=n_pay),
        "amount": [round(random.choice([0.99, 2.99, 4.99, 5.99, 7.99, 9.99, 11.99]), 2) for _ in range(n_pay)],
        "payment_date": [rand_datetime(datetime(2023,1,1), datetime(2024,12,31)) for _ in range(n_pay)],
    })

    return tables


# ============================================================
# 4. TPC-H DATA GENERATION
# ============================================================
def gen_tpch():
    tables = {}

    # Region
    region_names = ["AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"]
    tables["tpch_region"] = pl.DataFrame({
        "r_regionkey": list(range(len(region_names))),
        "r_name": region_names,
        "r_comment": [f"Region comment {i}" for i in range(len(region_names))],
    })

    # Nation
    nations = [("ALGERIA",0),("ARGENTINA",1),("BRAZIL",1),("CANADA",1),("EGYPT",0),
               ("ETHIOPIA",0),("FRANCE",3),("GERMANY",3),("INDIA",2),("INDONESIA",2),
               ("IRAN",4),("IRAQ",4),("JAPAN",2),("JORDAN",4),("KENYA",0),
               ("MOROCCO",0),("MOZAMBIQUE",0),("PERU",1),("CHINA",2),("ROMANIA",3),
               ("SAUDI ARABIA",4),("VIETNAM",2),("RUSSIA",3),("UNITED KINGDOM",3),("UNITED STATES",1)]
    tables["tpch_nation"] = pl.DataFrame({
        "n_nationkey": list(range(len(nations))),
        "n_name": [n[0] for n in nations],
        "n_regionkey": [n[1] for n in nations],
        "n_comment": [f"Nation comment {i}" for i in range(len(nations))],
    })

    # Supplier
    n_sup = 100
    tables["tpch_supplier"] = pl.DataFrame({
        "s_suppkey": list(range(1, n_sup+1)),
        "s_name": [f"Supplier#{str(i).zfill(9)}" for i in range(1, n_sup+1)],
        "s_address": [f"{random.randint(1,999)} Street_{random.randint(1,50)}" for _ in range(n_sup)],
        "s_nationkey": random.choices(range(len(nations)), k=n_sup),
        "s_phone": [f"{random.randint(10,34)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(n_sup)],
        "s_acctbal": [round(random.uniform(-999, 9999), 2) for _ in range(n_sup)],
        "s_comment": [f"Supplier comment {i}" for i in range(1, n_sup+1)],
    })

    # Part
    n_parts = 500
    mfgrs = [f"Manufacturer#{i}" for i in range(1, 6)]
    brands = [f"Brand#{i}{j}" for i in range(1,6) for j in range(1,6)]
    types_prefix = ["STANDARD","SMALL","MEDIUM","LARGE","ECONOMY","PROMO"]
    types_mat = ["POLISHED","BRUSHED","BURNISHED","PLATED","ANODIZED"]
    types_suf = ["TIN","NICKEL","BRASS","STEEL","COPPER"]
    containers = ["SM CASE","SM BOX","SM PACK","SM PKG","MED BAG","MED BOX","MED PKG","LG CASE","LG BOX","LG PACK","LG DRUM","WRAP CASE"]
    tables["tpch_part"] = pl.DataFrame({
        "p_partkey": list(range(1, n_parts+1)),
        "p_name": [f"Part_{i}" for i in range(1, n_parts+1)],
        "p_mfgr": random.choices(mfgrs, k=n_parts),
        "p_brand": random.choices(brands, k=n_parts),
        "p_type": [f"{random.choice(types_prefix)} {random.choice(types_mat)} {random.choice(types_suf)}" for _ in range(n_parts)],
        "p_size": [random.randint(1, 50) for _ in range(n_parts)],
        "p_container": random.choices(containers, k=n_parts),
        "p_retailprice": [round(random.uniform(900, 2100), 2) for _ in range(n_parts)],
        "p_comment": [f"Part comment {i}" for i in range(1, n_parts+1)],
    })

    # PartSupp
    ps_data = []
    for pk in range(1, n_parts+1):
        for sk in random.sample(range(1, n_sup+1), 4):
            ps_data.append({
                "ps_partkey": pk, "ps_suppkey": sk,
                "ps_availqty": random.randint(1, 9999),
                "ps_supplycost": round(random.uniform(1, 1000), 2),
                "ps_comment": f"PS comment {pk}-{sk}",
            })
    tables["tpch_partsupp"] = pl.DataFrame(ps_data)

    # Customer
    n_cust = 500
    mktsegments = ["AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"]
    tables["tpch_customer"] = pl.DataFrame({
        "c_custkey": list(range(1, n_cust+1)),
        "c_name": [f"Customer#{str(i).zfill(9)}" for i in range(1, n_cust+1)],
        "c_address": [f"{random.randint(1,9999)} Addr_{random.randint(1,200)}" for _ in range(n_cust)],
        "c_nationkey": random.choices(range(len(nations)), k=n_cust),
        "c_phone": [f"{random.randint(10,34)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(n_cust)],
        "c_acctbal": [round(random.uniform(-999, 9999), 2) for _ in range(n_cust)],
        "c_mktsegment": random.choices(mktsegments, k=n_cust),
        "c_comment": [f"Cust comment {i}" for i in range(1, n_cust+1)],
    })

    # Orders
    n_orders = 3000
    priorities = ["1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"]
    tables["tpch_orders"] = pl.DataFrame({
        "o_orderkey": list(range(1, n_orders+1)),
        "o_custkey": random.choices(range(1, n_cust+1), k=n_orders),
        "o_orderstatus": random.choices(["O","F","P"], weights=[0.25, 0.5, 0.25], k=n_orders),
        "o_totalprice": [round(random.uniform(500, 500000), 2) for _ in range(n_orders)],
        "o_orderdate": [rand_date(datetime(2020,1,1), datetime(2024,12,31)) for _ in range(n_orders)],
        "o_orderpriority": random.choices(priorities, k=n_orders),
        "o_clerk": [f"Clerk#{str(random.randint(1,1000)).zfill(9)}" for _ in range(n_orders)],
        "o_shippriority": [0] * n_orders,
        "o_comment": [f"Order comment {i}" for i in range(1, n_orders+1)],
    })

    # Lineitem
    li_data = []
    for ok in range(1, n_orders+1):
        n_lines = random.randint(1, 7)
        for ln in range(1, n_lines+1):
            pk = random.randint(1, n_parts)
            sk = random.randint(1, n_sup)
            qty = round(random.uniform(1, 50), 2)
            price = round(random.uniform(900, 100000)/100, 2)
            disc = round(random.uniform(0, 0.10), 2)
            tax = round(random.uniform(0, 0.08), 2)
            ship_d = rand_date(datetime(2020,1,15), datetime(2025,6,30))
            li_data.append({
                "l_orderkey": ok, "l_partkey": pk, "l_suppkey": sk, "l_linenumber": ln,
                "l_quantity": qty, "l_extendedprice": round(qty * price, 2),
                "l_discount": disc, "l_tax": tax,
                "l_returnflag": random.choice(["R","A","N"]),
                "l_linestatus": random.choice(["O","F"]),
                "l_shipdate": ship_d,
                "l_commitdate": rand_date(datetime(2020,1,10), datetime(2025,6,25)),
                "l_receiptdate": rand_date(datetime(2020,1,20), datetime(2025,7,10)),
                "l_shipinstruct": random.choice(["DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"]),
                "l_shipmode": random.choice(["REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"]),
                "l_comment": f"LI comment {ok}-{ln}",
            })
    tables["tpch_lineitem"] = pl.DataFrame(li_data)

    return tables


# ============================================================
# 5. SPIDER MINI DATA GENERATION
# ============================================================
def gen_spider():
    tables = {}

    # Concert Singer
    n_singers = 20
    tables["sp_singer"] = pl.DataFrame({
        "singer_id": list(range(1, n_singers+1)),
        "name": [rand_name() for _ in range(n_singers)],
        "country": random.choices(COUNTRIES_EU, k=n_singers),
        "song_name": [f"Song_{i}" for i in range(1, n_singers+1)],
        "song_release_year": [str(random.randint(1990, 2024)) for _ in range(n_singers)],
        "age": [random.randint(20, 65) for _ in range(n_singers)],
        "is_male": random.choices([True, False], k=n_singers),
    })

    n_stadiums = 10
    tables["sp_stadium"] = pl.DataFrame({
        "stadium_id": list(range(1, n_stadiums+1)),
        "location": [random.choice(CITIES_MAP[random.choice(COUNTRIES_EU)]) for _ in range(n_stadiums)],
        "name": [f"Stadium_{i}" for i in range(1, n_stadiums+1)],
        "capacity": [random.randint(5000, 80000) for _ in range(n_stadiums)],
        "average_attendance": [random.randint(3000, 60000) for _ in range(n_stadiums)],
    })

    n_concerts = 30
    tables["sp_concert"] = pl.DataFrame({
        "concert_id": list(range(1, n_concerts+1)),
        "concert_name": [f"Concert_{i}" for i in range(1, n_concerts+1)],
        "theme": random.choices(["Autumn","Free","Wild","Summer","Rock","Jazz","Pop","Classical"], k=n_concerts),
        "stadium_id": random.choices(range(1, n_stadiums+1), k=n_concerts),
        "year": [str(random.randint(2020, 2025)) for _ in range(n_concerts)],
    })

    perf_data = []
    for cid in range(1, n_concerts+1):
        for sid in random.sample(range(1, n_singers+1), random.randint(1, 5)):
            perf_data.append({"singer_id": sid, "concert_id": cid})
    tables["sp_performance"] = pl.DataFrame(perf_data)

    # World
    world_countries_data = [
        ("FRA","France","Europe","Western Europe",551695,67390000,82.5,2583560,"Republic"),
        ("DEU","Germany","Europe","Western Europe",357022,83784000,81.0,3846414,"Federal Republic"),
        ("ESP","Spain","Europe","Southern Europe",505992,46755000,83.4,1281199,"Constitutional Monarchy"),
        ("ITA","Italy","Europe","Southern Europe",301340,60462000,83.5,1886445,"Republic"),
        ("GBR","United Kingdom","Europe","British Islands",243610,67886000,81.3,2827113,"Constitutional Monarchy"),
        ("USA","United States","North America","North America",9833520,331003000,78.9,21433226,"Federal Republic"),
        ("BRA","Brazil","South America","South America",8515767,212559000,75.9,1839758,"Federal Republic"),
        ("JPN","Japan","Asia","Eastern Asia",377975,126476000,84.6,5082465,"Constitutional Monarchy"),
        ("CHN","China","Asia","Eastern Asia",9596961,1439324000,76.9,14342903,"People's Republic"),
        ("IND","India","Asia","Southern Asia",3287263,1380004000,69.4,2875142,"Federal Republic"),
        ("AUS","Australia","Oceania","Australia and New Zealand",7692024,25499884,82.8,1392681,"Constitutional Monarchy"),
        ("CAN","Canada","North America","North America",9984670,37742154,82.3,1736426,"Constitutional Monarchy"),
        ("MEX","Mexico","North America","Central America",1964375,128933000,75.0,1268871,"Federal Republic"),
        ("NGA","Nigeria","Africa","Western Africa",923768,206140000,54.7,448120,"Federal Republic"),
        ("ZAF","South Africa","Africa","Southern Africa",1221037,59309000,64.1,351432,"Republic"),
        ("KOR","South Korea","Asia","Eastern Asia",100210,51269000,82.8,1642383,"Republic"),
        ("RUS","Russia","Europe","Eastern Europe",17098242,145934000,72.6,1699877,"Federal Republic"),
        ("ARG","Argentina","South America","South America",2780400,45196000,76.5,449663,"Federal Republic"),
        ("EGY","Egypt","Africa","Northern Africa",1002450,102334000,71.8,303175,"Republic"),
        ("THA","Thailand","Asia","Southeast Asia",513120,69800000,76.9,543650,"Constitutional Monarchy"),
    ]
    tables["sp_world_country"] = pl.DataFrame({
        "code": [w[0] for w in world_countries_data],
        "name": [w[1] for w in world_countries_data],
        "continent": [w[2] for w in world_countries_data],
        "region": [w[3] for w in world_countries_data],
        "surface_area": [float(w[4]) for w in world_countries_data],
        "population": [w[5] for w in world_countries_data],
        "life_expectancy": [w[6] for w in world_countries_data],
        "gnp": [float(w[7]) for w in world_countries_data],
        "government_form": [w[8] for w in world_countries_data],
        "head_of_state": [f"Leader_{w[0]}" for w in world_countries_data],
    })

    # World cities
    world_cities = []
    cid = 1
    for wc in world_countries_data:
        for i in range(random.randint(3, 8)):
            world_cities.append({"city_id": cid, "name": f"{wc[1]}_City_{i+1}",
                                 "country_code": wc[0], "district": f"District_{random.randint(1,10)}",
                                 "population": random.randint(50000, 15000000)})
            cid += 1
    tables["sp_world_city"] = pl.DataFrame(world_cities)

    # World languages
    lang_data = []
    for wc in world_countries_data:
        n_lang = random.randint(1, 4)
        langs = random.sample(["English","French","Spanish","German","Arabic","Chinese","Hindi",
                               "Portuguese","Japanese","Korean","Russian","Thai","Italian","Dutch"], n_lang)
        remaining = 100.0
        for i, l in enumerate(langs):
            pct = round(remaining * random.uniform(0.3, 0.9), 1) if i < n_lang - 1 else round(remaining, 1)
            remaining -= pct
            lang_data.append({"country_code": wc[0], "language": l,
                              "is_official": i == 0, "percentage": pct})
    tables["sp_world_language"] = pl.DataFrame(lang_data)

    # Pets
    n_students = 30
    tables["sp_student"] = pl.DataFrame({
        "stu_id": list(range(1, n_students+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_students)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_students)],
        "age": [random.randint(18, 28) for _ in range(n_students)],
        "sex": random.choices(["M","F"], k=n_students),
        "major": random.choices([600, 520, 540, 100, 200, 300], k=n_students),
        "advisor": random.choices(range(1101, 1120), k=n_students),
        "city_code": random.choices(["NYC","CHI","LAX","HOU","PHX","SAN"], k=n_students),
    })

    n_pets = 20
    tables["sp_pet"] = pl.DataFrame({
        "pet_id": list(range(1, n_pets+1)),
        "pet_type": random.choices(["cat","dog","hamster","fish","parrot","rabbit"], k=n_pets),
        "pet_age": [random.randint(1, 15) for _ in range(n_pets)],
        "weight": [round(random.uniform(0.5, 40), 1) for _ in range(n_pets)],
    })

    hp_data = []
    for sid in range(1, n_students+1):
        if random.random() > 0.3:
            for pid in random.sample(range(1, n_pets+1), random.randint(1, 3)):
                hp_data.append({"stu_id": sid, "pet_id": pid})
    tables["sp_has_pet"] = pl.DataFrame(hp_data)

    # Cars
    makers = [("Toyota","Toyota Motor Corporation","Japan"),("Ford","Ford Motor Company","USA"),
              ("BMW","Bayerische Motoren Werke","Germany"),("Honda","Honda Motor Co.","Japan"),
              ("Volkswagen","Volkswagen AG","Germany"),("Fiat","Fiat Chrysler","Italy"),
              ("Peugeot","Stellantis","France"),("Hyundai","Hyundai Motor","South Korea")]
    tables["sp_car_maker"] = pl.DataFrame({
        "id": list(range(1, len(makers)+1)),
        "maker": [m[0] for m in makers],
        "full_name": [m[1] for m in makers],
        "country": [m[2] for m in makers],
    })

    n_cars = 100
    tables["sp_car_data"] = pl.DataFrame({
        "car_id": list(range(1, n_cars+1)),
        "maker_id": random.choices(range(1, len(makers)+1), k=n_cars),
        "model": [f"Model_{i}" for i in range(1, n_cars+1)],
        "mpg": [round(random.uniform(12, 50), 1) for _ in range(n_cars)],
        "cylinders": random.choices([4, 4, 4, 6, 6, 8], k=n_cars),
        "horsepower": [round(random.uniform(60, 350), 0) if random.random() > 0.05 else None for _ in range(n_cars)],
        "weight": [random.randint(1800, 5000) for _ in range(n_cars)],
        "accelerate": [round(random.uniform(8, 22), 1) for _ in range(n_cars)],
        "year": random.choices(range(2010, 2025), k=n_cars),
    })

    # Flights
    airports_data = [
        ("Charles de Gaulle","Paris","France","CDG","LFPG",392),
        ("Heathrow","London","UK","LHR","EGLL",83),
        ("Frankfurt","Frankfurt","Germany","FRA","EDDF",364),
        ("Schiphol","Amsterdam","Netherlands","AMS","EHAM",-11),
        ("Barajas","Madrid","Spain","MAD","LEMD",2000),
        ("JFK","New York","USA","JFK","KJFK",13),
        ("LAX","Los Angeles","USA","LAX","KLAX",126),
        ("Narita","Tokyo","Japan","NRT","RJAA",141),
        ("Sydney","Sydney","Australia","SYD","YSSY",21),
        ("Dubai","Dubai","UAE","DXB","OMDB",62),
        ("O'Hare","Chicago","USA","ORD","KORD",672),
        ("Changi","Singapore","Singapore","SIN","WSSS",22),
    ]
    tables["sp_airport"] = pl.DataFrame({
        "airport_id": list(range(1, len(airports_data)+1)),
        "airport_name": [a[0] for a in airports_data],
        "city": [a[1] for a in airports_data],
        "country": [a[2] for a in airports_data],
        "iata": [a[3] for a in airports_data],
        "icao": [a[4] for a in airports_data],
        "altitude": [a[5] for a in airports_data],
    })

    airlines_data = [
        ("Air France","AF","AFR","France",True),("Lufthansa","LH","DLH","Germany",True),
        ("British Airways","BA","BAW","UK",True),("KLM","KL","KLM","Netherlands",True),
        ("Iberia","IB","IBE","Spain",True),("Ryanair","FR","RYR","Ireland",True),
        ("EasyJet","U2","EZY","UK",True),("Emirates","EK","UAE","UAE",True),
        ("Delta","DL","DAL","USA",True),("United","UA","UAL","USA",True),
        ("ANA","NH","ANA","Japan",True),("Qantas","QF","QFA","Australia",True),
        ("OldAir",None,None,"France",False),
    ]
    tables["sp_airline"] = pl.DataFrame({
        "airline_id": list(range(1, len(airlines_data)+1)),
        "name": [a[0] for a in airlines_data],
        "iata": [a[1] for a in airlines_data],
        "icao": [a[2] for a in airlines_data],
        "country": [a[3] for a in airlines_data],
        "active": [a[4] for a in airlines_data],
    })

    n_routes = 200
    n_ap = len(airports_data)
    n_al = len(airlines_data)
    tables["sp_route"] = pl.DataFrame({
        "route_id": list(range(1, n_routes+1)),
        "airline_id": random.choices(range(1, n_al+1), k=n_routes),
        "src_airport_id": random.choices(range(1, n_ap+1), k=n_routes),
        "dst_airport_id": random.choices(range(1, n_ap+1), k=n_routes),
        "codeshare": random.choices([True, False], weights=[0.2, 0.8], k=n_routes),
        "stops": random.choices([0, 0, 0, 0, 1, 1, 2], k=n_routes),
    })

    return tables


# ============================================================
# 6. QUESTIONS — SELECTION FOCUSED (~120 questions)
# ============================================================
def build_questions():
    questions = []
    def q(qid, cat, diff, tables_used, question, code):
        questions.append({"id": qid, "category": cat, "difficulty": diff,
                          "tables_used": tables_used, "question_natural_language": question,
                          "gold_code": code})

    # ── NORTHWIND ────────────────────────────────────────────

    q("NW01","select_filter","easy","nw_products",
      "Find all discontinued products.",
      'nw_products.filter(pl.col("discontinued"))')

    q("NW02","select_filter","easy","nw_customers",
      "Select customer_id, company_name, and country for all customers in France.",
      'nw_customers.filter(pl.col("country") == "France").select("customer_id", "company_name", "country")')

    q("NW03","select_filter","medium","nw_products",
      "Find products with unit_price above 50 that are not discontinued and have units_in_stock below their reorder_level.",
      'nw_products.filter((pl.col("unit_price") > 50) & ~pl.col("discontinued") & (pl.col("units_in_stock") < pl.col("reorder_level")))')

    q("NW04","aggregation","easy","nw_products",
      "Count the number of products per category.",
      'nw_products.group_by("category_id").agg(pl.len().alias("product_count"))')

    q("NW05","aggregation","medium","nw_order_details",
      "Calculate total revenue (unit_price * quantity * (1 - discount)) per order.",
      'nw_order_details.with_columns((pl.col("unit_price") * pl.col("quantity") * (1 - pl.col("discount"))).alias("revenue")).group_by("order_id").agg(pl.col("revenue").sum().round(2).alias("total_revenue"))')

    q("NW06","joins","medium","nw_products,nw_categories",
      "List all products with their category name. Select product_name, category_name, unit_price.",
      'nw_products.join(nw_categories, on="category_id").select("product_name", "category_name", "unit_price")')

    q("NW07","joins","medium","nw_orders,nw_customers",
      "Find all orders placed by customers from Germany. Return order_id, company_name, order_date.",
      'nw_orders.join(nw_customers, on="customer_id").filter(pl.col("country") == "Germany").select("order_id", "company_name", "order_date")')

    q("NW08","joins","hard","nw_order_details,nw_products,nw_categories",
      "Calculate total revenue per product category. Join order_details→products→categories.",
      'nw_order_details.join(nw_products, on="product_id").join(nw_categories, on="category_id").with_columns((pl.col("unit_price") * pl.col("quantity") * (1 - pl.col("discount"))).alias("revenue")).group_by("category_name").agg(pl.col("revenue").sum().round(2).alias("total_revenue")).sort("total_revenue", descending=True)')

    q("NW09","joins","hard","nw_orders,nw_order_details,nw_products,nw_customers",
      "Find the top 5 customers by total spending. Return company_name, country, total_spent.",
      'nw_order_details.with_columns((pl.col("unit_price") * pl.col("quantity") * (1 - pl.col("discount"))).alias("revenue")).group_by("order_id").agg(pl.col("revenue").sum().alias("order_total")).join(nw_orders, on="order_id").group_by("customer_id").agg(pl.col("order_total").sum().round(2).alias("total_spent")).join(nw_customers, on="customer_id").select("company_name", "country", "total_spent").sort("total_spent", descending=True).head(5)')

    q("NW10","aggregation","medium","nw_orders",
      "Count orders per shipper.",
      'nw_orders.group_by("shipper_id").agg(pl.len().alias("order_count"))')

    q("NW11","select_filter","medium","nw_orders",
      "Find all orders that have not been shipped yet (shipped_date is null).",
      'nw_orders.filter(pl.col("shipped_date").is_null())')

    q("NW12","aggregation","hard","nw_orders,nw_employees",
      "Find the employee who handled the most orders. Return first_name, last_name, order_count.",
      'nw_orders.group_by("employee_id").agg(pl.len().alias("order_count")).sort("order_count", descending=True).head(1).join(nw_employees, on="employee_id").select("first_name", "last_name", "order_count")')

    q("NW13","joins","easy","nw_products,nw_suppliers",
      "List each product with its supplier company name.",
      'nw_products.join(nw_suppliers, on="supplier_id").select("product_name", "company_name")')

    q("NW14","aggregation","medium","nw_orders",
      "Calculate the average freight cost per country (join with customers).",
      'nw_orders.join(nw_customers, on="customer_id").group_by("country").agg(pl.col("freight").mean().round(2).alias("avg_freight"))')

    q("NW15","joins","hard","nw_customers,nw_orders",
      "Find customers who have never placed an order.",
      'nw_customers.join(nw_orders, on="customer_id", how="anti")')

    q("NW16","select_filter","easy","nw_suppliers",
      "Find all suppliers from Italy.",
      'nw_suppliers.filter(pl.col("country") == "Italy")')

    q("NW17","aggregation","hard","nw_order_details",
      "Find the product with the highest total quantity ever ordered.",
      'nw_order_details.group_by("product_id").agg(pl.col("quantity").sum().alias("total_qty")).sort("total_qty", descending=True).head(1)')

    q("NW18","window","medium","nw_orders",
      "Rank orders by freight cost within each shipper (most expensive first).",
      'nw_orders.with_columns(pl.col("freight").rank(method="dense", descending=True).over("shipper_id").alias("freight_rank"))')

    # ── CHINOOK ──────────────────────────────────────────────

    q("CK01","select_filter","easy","ck_tracks",
      "Find all tracks priced at 1.99.",
      'ck_tracks.filter(pl.col("unit_price") == 1.99)')

    q("CK02","joins","easy","ck_tracks,ck_albums",
      "List all tracks with their album title.",
      'ck_tracks.join(ck_albums, on="album_id").select("name", "title")')

    q("CK03","joins","medium","ck_tracks,ck_albums,ck_artists",
      "List all tracks with their artist name. Join tracks→albums→artists.",
      'ck_tracks.join(ck_albums, on="album_id").join(ck_artists, on="artist_id").select(pl.col("name").alias("track_name"), pl.col("name_right").alias("artist_name"))')

    q("CK04","aggregation","easy","ck_tracks",
      "Count the number of tracks per genre.",
      'ck_tracks.group_by("genre_id").agg(pl.len().alias("track_count"))')

    q("CK05","aggregation","medium","ck_tracks,ck_genres",
      "Count tracks per genre name. Join with genres. Sort by count descending.",
      'ck_tracks.join(ck_genres, on="genre_id").group_by("name").agg(pl.len().alias("track_count")).sort("track_count", descending=True)')

    q("CK06","aggregation","medium","ck_albums,ck_artists",
      "Find artists with the most albums. Return artist name and album count. Top 5.",
      'ck_albums.group_by("artist_id").agg(pl.len().alias("album_count")).sort("album_count", descending=True).head(5).join(ck_artists, on="artist_id").select("name", "album_count")')

    q("CK07","aggregation","hard","ck_invoice_lines,ck_invoices",
      "Calculate total sales per billing country. Join invoice_lines→invoices. Revenue = unit_price * quantity.",
      'ck_invoice_lines.with_columns((pl.col("unit_price") * pl.col("quantity")).alias("line_total")).join(ck_invoices, on="invoice_id").group_by("billing_country").agg(pl.col("line_total").sum().round(2).alias("total_sales")).sort("total_sales", descending=True)')

    q("CK08","joins","hard","ck_invoice_lines,ck_tracks,ck_genres",
      "Find the most purchased genre by total quantity. Join invoice_lines→tracks→genres.",
      'ck_invoice_lines.join(ck_tracks, on="track_id").join(ck_genres, on="genre_id").group_by(pl.col("name_right").alias("genre")).agg(pl.col("quantity").sum().alias("total_purchased")).sort("total_purchased", descending=True).head(1)')

    q("CK09","select_filter","medium","ck_tracks",
      "Find tracks longer than 5 minutes (300000 ms) with no composer listed.",
      'ck_tracks.filter((pl.col("milliseconds") > 300000) & pl.col("composer").is_null())')

    q("CK10","aggregation","medium","ck_invoices",
      "Calculate total invoice amount per customer.",
      'ck_invoices.group_by("customer_id").agg(pl.col("total").sum().round(2).alias("total_spent"))')

    q("CK11","joins","medium","ck_customers,ck_employees",
      "List each customer with their support rep name. Select customer first_name, last_name, rep first_name.",
      'ck_customers.join(ck_employees, left_on="support_rep_id", right_on="employee_id").select(pl.col("first_name").alias("cust_first"), pl.col("last_name").alias("cust_last"), pl.col("first_name_right").alias("rep_first"))')

    q("CK12","aggregation","hard","ck_invoices,ck_customers",
      "Find the top spending customer per country. Return country, customer first_name, total.",
      'ck_invoices.group_by("customer_id").agg(pl.col("total").sum().round(2).alias("total_spent")).join(ck_customers, on="customer_id").sort("total_spent", descending=True).group_by("country").agg(pl.col("first_name").first(), pl.col("total_spent").first())')

    q("CK13","select_filter","easy","ck_customers",
      "Find all customers from France.",
      'ck_customers.filter(pl.col("country") == "France")')

    q("CK14","aggregation","easy","ck_invoices",
      "Count invoices per year. Extract year from invoice_date.",
      'ck_invoices.with_columns(pl.col("invoice_date").str.slice(0, 4).alias("year")).group_by("year").agg(pl.len().alias("invoice_count")).sort("year")')

    q("CK15","joins","medium","ck_playlist_track,ck_playlists",
      "Count the number of tracks in each playlist. Return playlist name and track_count.",
      'ck_playlist_track.group_by("playlist_id").agg(pl.len().alias("track_count")).join(ck_playlists, on="playlist_id").select("name", "track_count").sort("track_count", descending=True)')

    q("CK16","select_filter","medium","ck_tracks",
      "Find the average track duration in minutes per genre.",
      'ck_tracks.with_columns((pl.col("milliseconds") / 60000).round(2).alias("duration_min")).group_by("genre_id").agg(pl.col("duration_min").mean().round(2).alias("avg_duration_min"))')

    # ── SAKILA ───────────────────────────────────────────────

    q("SK01","select_filter","easy","sk_films",
      "Find all R-rated films.",
      'sk_films.filter(pl.col("rating") == "R")')

    q("SK02","select_filter","medium","sk_films",
      "Find films longer than 2 hours with rental rate above 2.99.",
      'sk_films.filter((pl.col("length_min") > 120) & (pl.col("rental_rate") > 2.99))')

    q("SK03","aggregation","easy","sk_films",
      "Count films per rating.",
      'sk_films.group_by("rating").agg(pl.len().alias("film_count"))')

    q("SK04","joins","medium","sk_films,sk_film_category,sk_categories",
      "List all films with their category name. Select title, category name, rating.",
      'sk_films.join(sk_film_category, on="film_id").join(sk_categories, on="category_id").select("title", "name", "rating")')

    q("SK05","joins","hard","sk_film_actor,sk_actors,sk_films",
      "Find actors who have appeared in more than 20 films. Return actor name and film count.",
      'sk_film_actor.group_by("actor_id").agg(pl.len().alias("film_count")).filter(pl.col("film_count") > 20).join(sk_actors, on="actor_id").select("first_name", "last_name", "film_count").sort("film_count", descending=True)')

    q("SK06","aggregation","medium","sk_payments",
      "Calculate total payment amount per customer.",
      'sk_payments.group_by("customer_id").agg(pl.col("amount").sum().round(2).alias("total_paid"))')

    q("SK07","aggregation","hard","sk_payments,sk_customers",
      "Find the top 10 customers by total payments. Return first_name, last_name, total_paid.",
      'sk_payments.group_by("customer_id").agg(pl.col("amount").sum().round(2).alias("total_paid")).sort("total_paid", descending=True).head(10).join(sk_customers, on="customer_id").select("first_name", "last_name", "total_paid")')

    q("SK08","select_filter","easy","sk_rentals",
      "Find all rentals that have not been returned (return_date is null).",
      'sk_rentals.filter(pl.col("return_date").is_null())')

    q("SK09","aggregation","medium","sk_rentals",
      "Count rentals per staff member.",
      'sk_rentals.group_by("staff_id").agg(pl.len().alias("rental_count"))')

    q("SK10","joins","hard","sk_rentals,sk_inventory,sk_films",
      "Find the most rented film. Join rentals→inventory→films. Return title and rental_count.",
      'sk_rentals.join(sk_inventory, on="inventory_id").group_by("film_id").agg(pl.len().alias("rental_count")).sort("rental_count", descending=True).head(1).join(sk_films, on="film_id").select("title", "rental_count")')

    q("SK11","aggregation","hard","sk_films,sk_film_category,sk_categories",
      "Calculate the average film length per category. Return category name and avg_length.",
      'sk_films.join(sk_film_category, on="film_id").join(sk_categories, on="category_id").group_by("name").agg(pl.col("length_min").mean().round(1).alias("avg_length")).sort("avg_length", descending=True)')

    q("SK12","joins","medium","sk_inventory,sk_films",
      "Count inventory copies per film per store. Return film title, store_id, copy_count.",
      'sk_inventory.group_by("film_id", "store_id").agg(pl.len().alias("copy_count")).join(sk_films, on="film_id").select("title", "store_id", "copy_count").sort("copy_count", descending=True)')

    q("SK13","joins","hard","sk_customers,sk_addresses,sk_cities,sk_countries",
      "List customers with their country. Join customers→addresses→cities→countries. Select first_name, last_name, country.",
      'sk_customers.join(sk_addresses, on="address_id").join(sk_cities, on="city_id").join(sk_countries, on="country_id").select("first_name", "last_name", "country")')

    q("SK14","aggregation","hard","sk_payments",
      "Find the month with the highest total payments. Extract month from payment_date.",
      'sk_payments.with_columns(pl.col("payment_date").str.slice(0, 7).alias("month")).group_by("month").agg(pl.col("amount").sum().round(2).alias("total")).sort("total", descending=True).head(1)')

    q("SK15","select_filter","medium","sk_films",
      "Find films released after 2015 with a replacement cost above 25.",
      'sk_films.filter((pl.col("release_year") > 2015) & (pl.col("replacement_cost") > 25))')

    q("SK16","joins","easy","sk_films,sk_languages",
      "List films with their language name.",
      'sk_films.join(sk_languages, on="language_id").select("title", pl.col("name").alias("language"))')

    # ── TPC-H ────────────────────────────────────────────────

    q("TH01","select_filter","easy","tpch_lineitem",
      "Find all returned line items (l_returnflag = 'R').",
      'tpch_lineitem.filter(pl.col("l_returnflag") == "R")')

    q("TH02","aggregation","medium","tpch_lineitem",
      "TPC-H Q1 simplified: Group lineitem by l_returnflag and l_linestatus. Calculate sum of l_quantity, sum of l_extendedprice, avg of l_discount, count.",
      'tpch_lineitem.group_by("l_returnflag", "l_linestatus").agg(pl.col("l_quantity").sum().alias("sum_qty"), pl.col("l_extendedprice").sum().alias("sum_price"), pl.col("l_discount").mean().round(4).alias("avg_disc"), pl.len().alias("count_order")).sort("l_returnflag", "l_linestatus")')

    q("TH03","joins","medium","tpch_orders,tpch_customer",
      "Find all orders from customers in the AUTOMOBILE market segment.",
      'tpch_orders.join(tpch_customer, left_on="o_custkey", right_on="c_custkey").filter(pl.col("c_mktsegment") == "AUTOMOBILE").select("o_orderkey", "o_orderdate", "o_totalprice", "c_name")')

    q("TH04","joins","hard","tpch_orders,tpch_customer,tpch_nation",
      "Find total order value per nation. Join orders→customer→nation.",
      'tpch_orders.join(tpch_customer, left_on="o_custkey", right_on="c_custkey").join(tpch_nation, left_on="c_nationkey", right_on="n_nationkey").group_by("n_name").agg(pl.col("o_totalprice").sum().round(2).alias("total_value")).sort("total_value", descending=True)')

    q("TH05","aggregation","medium","tpch_supplier,tpch_nation",
      "Count suppliers per nation. Join with nation to get name.",
      'tpch_supplier.join(tpch_nation, left_on="s_nationkey", right_on="n_nationkey").group_by("n_name").agg(pl.len().alias("supplier_count")).sort("supplier_count", descending=True)')

    q("TH06","select_filter","medium","tpch_lineitem",
      "TPC-H Q6: Find lineitems shipped between 2023-01-01 and 2023-12-31 with discount between 0.05 and 0.07 and quantity < 24.",
      'tpch_lineitem.filter((pl.col("l_shipdate") >= "2023-01-01") & (pl.col("l_shipdate") <= "2023-12-31") & (pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07) & (pl.col("l_quantity") < 24))')

    q("TH07","aggregation","hard","tpch_lineitem",
      "Calculate total revenue lost to discounts: sum of l_extendedprice * l_discount.",
      'tpch_lineitem.select((pl.col("l_extendedprice") * pl.col("l_discount")).sum().round(2).alias("revenue_lost"))')

    q("TH08","joins","hard","tpch_lineitem,tpch_orders,tpch_customer,tpch_nation,tpch_region",
      "TPC-H Q5 simplified: Total revenue per nation for customers in EUROPE region. Revenue = l_extendedprice * (1 - l_discount).",
      'tpch_lineitem.with_columns((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")).join(tpch_orders, left_on="l_orderkey", right_on="o_orderkey").join(tpch_customer, left_on="o_custkey", right_on="c_custkey").join(tpch_nation, left_on="c_nationkey", right_on="n_nationkey").join(tpch_region, left_on="n_regionkey", right_on="r_regionkey").filter(pl.col("r_name") == "EUROPE").group_by("n_name").agg(pl.col("revenue").sum().round(2).alias("total_revenue")).sort("total_revenue", descending=True)')

    q("TH09","aggregation","medium","tpch_part",
      "Count parts per manufacturer.",
      'tpch_part.group_by("p_mfgr").agg(pl.len().alias("part_count")).sort("part_count", descending=True)')

    q("TH10","joins","medium","tpch_partsupp,tpch_supplier",
      "Find the supplier with the lowest supply cost for each part.",
      'tpch_partsupp.sort("ps_supplycost").group_by("ps_partkey").agg(pl.col("ps_suppkey").first().alias("best_supplier"), pl.col("ps_supplycost").first().alias("min_cost"))')

    q("TH11","aggregation","hard","tpch_orders",
      "Count orders per priority per year. Extract year from o_orderdate.",
      'tpch_orders.with_columns(pl.col("o_orderdate").str.slice(0, 4).alias("year")).group_by("year", "o_orderpriority").agg(pl.len().alias("order_count")).sort("year", "o_orderpriority")')

    q("TH12","select_filter","easy","tpch_customer",
      "Find all customers in the BUILDING market segment with positive account balance.",
      'tpch_customer.filter((pl.col("c_mktsegment") == "BUILDING") & (pl.col("c_acctbal") > 0))')

    q("TH13","aggregation","medium","tpch_lineitem",
      "Calculate average quantity and average extended price per ship mode.",
      'tpch_lineitem.group_by("l_shipmode").agg(pl.col("l_quantity").mean().round(2).alias("avg_qty"), pl.col("l_extendedprice").mean().round(2).alias("avg_price"))')

    q("TH14","joins","hard","tpch_lineitem,tpch_part",
      "Find total revenue per part brand. Revenue = l_extendedprice * (1 - l_discount). Join lineitem with part.",
      'tpch_lineitem.with_columns((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")).join(tpch_part, left_on="l_partkey", right_on="p_partkey").group_by("p_brand").agg(pl.col("revenue").sum().round(2).alias("total_revenue")).sort("total_revenue", descending=True)')

    # ── SPIDER MINI ──────────────────────────────────────────

    q("SP01","select_filter","easy","sp_singer",
      "Find all male singers older than 30.",
      'sp_singer.filter(pl.col("is_male") & (pl.col("age") > 30))')

    q("SP02","joins","medium","sp_performance,sp_singer,sp_concert",
      "List all performances: singer name, concert name, year.",
      'sp_performance.join(sp_singer, on="singer_id").join(sp_concert, on="concert_id").select(pl.col("name").alias("singer_name"), "concert_name", "year")')

    q("SP03","aggregation","medium","sp_performance",
      "Count the number of concerts each singer has performed in.",
      'sp_performance.group_by("singer_id").agg(pl.len().alias("concert_count")).sort("concert_count", descending=True)')

    q("SP04","joins","medium","sp_concert,sp_stadium",
      "List concerts with their stadium name and capacity.",
      'sp_concert.join(sp_stadium, on="stadium_id").select("concert_name", "theme", pl.col("name").alias("stadium_name"), "capacity")')

    q("SP05","joins","hard","sp_singer,sp_performance,sp_concert",
      "Find singers who have never performed at any concert (anti join).",
      'sp_singer.join(sp_performance, on="singer_id", how="anti")')

    q("SP06","select_filter","easy","sp_world_country",
      "Find all countries in Europe.",
      'sp_world_country.filter(pl.col("continent") == "Europe")')

    q("SP07","aggregation","medium","sp_world_country",
      "Find the total population per continent.",
      'sp_world_country.group_by("continent").agg(pl.col("population").sum().alias("total_pop")).sort("total_pop", descending=True)')

    q("SP08","select_filter","medium","sp_world_country",
      "Find countries with life expectancy above 80 and GNP above 1,000,000.",
      'sp_world_country.filter((pl.col("life_expectancy") > 80) & (pl.col("gnp") > 1000000))')

    q("SP09","joins","medium","sp_world_city,sp_world_country",
      "Find the most populated city per country. Return country name, city name, population.",
      'sp_world_city.sort("population", descending=True).group_by("country_code").agg(pl.col("name").first().alias("largest_city"), pl.col("population").first().alias("city_pop")).join(sp_world_country, left_on="country_code", right_on="code").select(pl.col("name").alias("country"), "largest_city", "city_pop")')

    q("SP10","joins","medium","sp_world_language,sp_world_country",
      "Find all official languages spoken in European countries.",
      'sp_world_language.filter(pl.col("is_official")).join(sp_world_country, left_on="country_code", right_on="code").filter(pl.col("continent") == "Europe").select(pl.col("name").alias("country"), "language", "percentage")')

    q("SP11","select_filter","easy","sp_pet",
      "Find all dogs weighing more than 10 kg.",
      'sp_pet.filter((pl.col("pet_type") == "dog") & (pl.col("weight") > 10))')

    q("SP12","joins","medium","sp_has_pet,sp_student,sp_pet",
      "List students and their pets. Select student first_name, pet_type, pet_age.",
      'sp_has_pet.join(sp_student, on="stu_id").join(sp_pet, on="pet_id").select("first_name", "pet_type", "pet_age")')

    q("SP13","joins","hard","sp_student,sp_has_pet",
      "Find students who do not own any pet.",
      'sp_student.join(sp_has_pet, on="stu_id", how="anti")')

    q("SP14","aggregation","medium","sp_has_pet,sp_pet",
      "Count pets per pet_type owned by students.",
      'sp_has_pet.join(sp_pet, on="pet_id").group_by("pet_type").agg(pl.len().alias("count")).sort("count", descending=True)')

    q("SP15","aggregation","medium","sp_car_data",
      "Find the average mpg per number of cylinders.",
      'sp_car_data.group_by("cylinders").agg(pl.col("mpg").mean().round(1).alias("avg_mpg")).sort("cylinders")')

    q("SP16","joins","medium","sp_car_data,sp_car_maker",
      "Find the average horsepower per car maker. Exclude nulls.",
      'sp_car_data.filter(pl.col("horsepower").is_not_null()).join(sp_car_maker, left_on="maker_id", right_on="id").group_by("maker").agg(pl.col("horsepower").mean().round(1).alias("avg_hp")).sort("avg_hp", descending=True)')

    q("SP17","select_filter","medium","sp_car_data",
      "Find cars with more than 200 horsepower and mpg above 20.",
      'sp_car_data.filter((pl.col("horsepower") > 200) & (pl.col("mpg") > 20))')

    q("SP18","aggregation","easy","sp_route",
      "Count routes per airline.",
      'sp_route.group_by("airline_id").agg(pl.len().alias("route_count")).sort("route_count", descending=True)')

    q("SP19","joins","medium","sp_route,sp_airline",
      "Find active airlines with the most routes. Return airline name and route count.",
      'sp_route.join(sp_airline, on="airline_id").filter(pl.col("active")).group_by("name").agg(pl.len().alias("route_count")).sort("route_count", descending=True)')

    q("SP20","joins","hard","sp_route,sp_airport",
      "Find the top 5 destination airports by number of incoming routes. Return airport_name, city, incoming_count.",
      'sp_route.group_by("dst_airport_id").agg(pl.len().alias("incoming_count")).sort("incoming_count", descending=True).head(5).join(sp_airport, left_on="dst_airport_id", right_on="airport_id").select("airport_name", "city", "incoming_count")')

    q("SP21","select_filter","easy","sp_airline",
      "Find all inactive airlines.",
      'sp_airline.filter(~pl.col("active"))')

    q("SP22","joins","hard","sp_route,sp_airport",
      "Find all direct routes (stops=0) from Paris CDG. Return destination airport name and airline_id.",
      'sp_route.filter(pl.col("stops") == 0).join(sp_airport, left_on="src_airport_id", right_on="airport_id").filter(pl.col("iata") == "CDG").join(sp_airport, left_on="dst_airport_id", right_on="airport_id", suffix="_dst").select("airline_id", pl.col("airport_name_dst").alias("destination"))')

    q("SP23","aggregation","medium","sp_world_country",
      "Find the average life expectancy per region.",
      'sp_world_country.group_by("region").agg(pl.col("life_expectancy").mean().round(1).alias("avg_life_exp")).sort("avg_life_exp", descending=True)')

    q("SP24","aggregation","hard","sp_car_data,sp_car_maker",
      "Find the country producing the most car models. Join car_data→car_maker. Return country and model_count.",
      'sp_car_data.join(sp_car_maker, left_on="maker_id", right_on="id").group_by("country").agg(pl.len().alias("model_count")).sort("model_count", descending=True).head(1)')

    return questions


# ============================================================
# 7. EXECUTE & HASH
# ============================================================
def execute_gold_code(code, datasets):
    local_vars = {**datasets, "pl": pl}
    lines = code.strip().split("\n")
    last = lines[-1].strip()
    assign_match = re.match(r'^([a-zA-Z_]\w*)\s*=\s*(?!=)', last)
    if assign_match:
        varname = assign_match.group(1)
        exec("\n".join(lines), {"__builtins__": __builtins__}, local_vars)
        return local_vars[varname]
    elif len(lines) == 1:
        return eval(code.strip(), {"__builtins__": __builtins__}, local_vars)
    else:
        exec("\n".join(lines[:-1]), {"__builtins__": __builtins__}, local_vars)
        return eval(last, {"__builtins__": __builtins__}, local_vars)

def result_hash(result):
    if isinstance(result, pl.DataFrame):
        return hashlib.sha256(result.sort(by=result.columns).write_csv().encode()).hexdigest()[:16]
    elif isinstance(result, pl.Series):
        return hashlib.sha256(str(result.to_list()).encode()).hexdigest()[:16]
    return hashlib.sha256(str(result).encode()).hexdigest()[:16]

def result_shape(result):
    if isinstance(result, pl.DataFrame):
        return {"rows": result.height, "cols": result.width, "columns": result.columns}
    return {}


# ============================================================
# 8. MAIN
# ============================================================
def main():
    print("=" * 60)
    print("SQL SCHEMAS → POLARS BENCHMARK GENERATOR")
    print("=" * 60)

    # Generate all datasets
    print("\n[1/4] Generating datasets...")
    all_tables = {}
    generators = [
        ("Northwind", gen_northwind),
        ("Chinook", gen_chinook),
        ("Sakila", gen_sakila),
        ("TPC-H", gen_tpch),
        ("Spider", gen_spider),
    ]
    for domain, gen_fn in generators:
        tables = gen_fn()
        for name, df in tables.items():
            filepath = DATASETS_DIR / f"{name}.parquet"
            df.write_parquet(str(filepath))
            all_tables[name] = df
            print(f"  ✓ [{domain:10s}] {name:25s}: {df.shape[0]:>8,} rows × {df.shape[1]:>2} cols")

    print(f"\n  Total: {len(all_tables)} tables")

    # Build questions
    print("\n[2/4] Building questions...")
    questions = build_questions()
    cat_counts = Counter(q["category"] for q in questions)
    diff_counts = Counter(q["difficulty"] for q in questions)
    print(f"  ✓ {len(questions)} questions")
    print(f"  Categories:   {dict(sorted(cat_counts.items()))}")
    print(f"  Difficulties: {dict(sorted(diff_counts.items()))}")

    # Execute gold evals
    print("\n[3/4] Executing gold evals...")
    errors = []
    for q in questions:
        try:
            result = execute_gold_code(q["gold_code"], all_tables)
            q["gold_hash"] = result_hash(result)
            q["gold_shape"] = result_shape(result)
            q["eval_status"] = "ok"
        except Exception as e:
            q["gold_hash"] = None
            q["gold_shape"] = None
            q["eval_status"] = f"ERROR: {e}"
            errors.append((q["id"], str(e)))
            print(f"  ✗ {q['id']}: {e}")

    ok = sum(1 for q in questions if q["eval_status"] == "ok")
    print(f"  ✓ {ok}/{len(questions)} passed")

    # Load schema metadata
    schema_file = Path("sql_schemas.json")
    schemas_meta = {}
    if schema_file.exists():
        with open(schema_file) as f:
            schemas_meta = json.load(f)

    # Save
    print("\n[4/4] Saving benchmark...")
    benchmark = {
        "metadata": {
            "name": "Polars SLM Benchmark — SQL Schemas Edition",
            "version": "1.0.0",
            "description": "Selection-focused benchmark based on classic SQL databases (Northwind, Chinook, Sakila, TPC-H, Spider).",
            "created": datetime.now().isoformat(),
            "polars_version": pl.__version__,
            "num_questions": len(questions),
            "num_tables": len(all_tables),
            "sources": ["Northwind","Chinook","Sakila","TPC-H","Spider"],
        },
        "schemas": schemas_meta.get("schemas", {}),
        "datasets": {
            name: {"file": f"datasets/{name}.parquet", "rows": df.shape[0], "cols": df.shape[1],
                   "columns": df.columns, "dtypes": {c: str(df[c].dtype) for c in df.columns}}
            for name, df in all_tables.items()
        },
        "questions": questions,
    }
    out = OUTPUT_DIR / "benchmark_sql.json"
    with open(out, "w") as f:
        json.dump(benchmark, f, indent=2, default=str)
    print(f"  ✓ {out}")

    # CSV summary
    q_df = pl.DataFrame([{"id": q["id"], "category": q["category"], "difficulty": q["difficulty"],
                          "tables": q["tables_used"], "question": q["question_natural_language"],
                          "status": q["eval_status"], "hash": q["gold_hash"]} for q in questions])
    csv_out = OUTPUT_DIR / "questions_sql_summary.csv"
    q_df.write_csv(str(csv_out))
    print(f"  ✓ {csv_out}")

    print(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    print(f"  Questions:  {len(questions)} ({ok} valid)")
    print(f"  Tables:     {len(all_tables)}")
    print(f"  Errors:     {len(errors)}")
    if errors:
        for qid, err in errors:
            print(f"    ✗ {qid}: {err}")
    print("\nDone!")

if __name__ == "__main__":
    main()
