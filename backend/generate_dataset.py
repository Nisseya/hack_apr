"""
Polars Benchmark — Dataset Generator (Large)
=============================================
Generates all 76 tables across 2 dataset groups:
  - Domain datasets (ecommerce, HR, IoT, academic, web, healthcare, finance, real_estate)
  - SQL schema datasets (Northwind, Chinook, Sakila, TPC-H, Spider)

Tables de référence : taille réaliste (5-50 rows)
Tables de faits : ×5 à ×10 par rapport à la version initiale

Usage:
    python generate_datasets.py
    
Output:
    datasets/
    ├── domain/     (17 tables)
    └── sql/        (59 tables)
"""

import polars as pl
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

SQL_DIR = Path("data")
SQL_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================
def rand_date(start, end):
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def rand_datetime(start, end):
    return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime("%Y-%m-%d %H:%M:%S")

def rand_phone():
    return f"+{random.randint(1,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

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

def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def save(df, directory, name):
    path = directory / f"{name}.parquet"
    df.write_parquet(str(path))
    print(f"  ✓ {name:30s}: {df.shape[0]:>10,} rows × {df.shape[1]:>2} cols")

# ============================================================
# SQL SCHEMA DATASETS
# ============================================================

def gen_sql():
    print("\n── SQL Schema Datasets ──")

    # ═══ NORTHWIND ═══
    cat_names = ["Beverages","Condiments","Confections","Dairy Products","Grains/Cereals","Meat/Poultry","Produce","Seafood"]
    save(pl.DataFrame({"category_id": list(range(1, len(cat_names)+1)), "category_name": cat_names,
        "description": [f"Description for {c}" for c in cat_names]}), SQL_DIR, "nw_categories")

    n_sup = 60
    scc = [rand_country_city() for _ in range(n_sup)]
    save(pl.DataFrame({
        "supplier_id": list(range(1, n_sup+1)),
        "company_name": [f"Supplier_{i} Co." for i in range(1, n_sup+1)],
        "contact_name": [rand_name() for _ in range(n_sup)],
        "contact_title": random.choices(["Sales Manager","Marketing Manager","Owner","Export Administrator","Sales Representative"], k=n_sup),
        "city": [c[1] for c in scc], "country": [c[0] for c in scc],
        "phone": [rand_phone() for _ in range(n_sup)],
    }), SQL_DIR, "nw_suppliers")

    n_prod = 200
    save(pl.DataFrame({
        "product_id": list(range(1, n_prod+1)),
        "product_name": [f"Product_{i}" for i in range(1, n_prod+1)],
        "supplier_id": random.choices(range(1, n_sup+1), k=n_prod),
        "category_id": random.choices(range(1, len(cat_names)+1), k=n_prod),
        "unit_price": [round(random.uniform(2, 250), 2) for _ in range(n_prod)],
        "units_in_stock": [random.randint(0, 200) for _ in range(n_prod)],
        "units_on_order": [random.randint(0, 80) for _ in range(n_prod)],
        "reorder_level": random.choices([0,5,10,15,20,25,30], k=n_prod),
        "discontinued": random.choices([True, False], weights=[0.1,0.9], k=n_prod),
    }), SQL_DIR, "nw_products")

    n_cust = 500
    cids = [f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))}" for _ in range(n_cust)]
    ccc = [rand_country_city() for _ in range(n_cust)]
    save(pl.DataFrame({
        "customer_id": cids,
        "company_name": [f"Company_{i}" for i in range(1, n_cust+1)],
        "contact_name": [rand_name() for _ in range(n_cust)],
        "contact_title": random.choices(["Owner","Sales Associate","Marketing Manager","Accounting Manager","Sales Representative","Order Administrator"], k=n_cust),
        "city": [c[1] for c in ccc], "country": [c[0] for c in ccc],
        "phone": [rand_phone() for _ in range(n_cust)],
        "fax": [rand_phone() if random.random()>0.4 else None for _ in range(n_cust)],
    }), SQL_DIR, "nw_customers")

    n_emp = 30
    save(pl.DataFrame({
        "employee_id": list(range(1, n_emp+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_emp)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_emp)],
        "title": random.choices(["Sales Representative","Sales Manager","Vice President Sales","Inside Sales Coordinator"], weights=[0.6,0.2,0.1,0.1], k=n_emp),
        "birth_date": [rand_date(datetime(1955,1,1), datetime(1990,12,31)) for _ in range(n_emp)],
        "hire_date": [rand_date(datetime(2015,1,1), datetime(2023,12,31)) for _ in range(n_emp)],
        "city": random.choices(["London","Seattle","Tacoma","Redmond","Kirkland"], k=n_emp),
        "country": random.choices(["UK","USA"], weights=[0.3,0.7], k=n_emp),
        "reports_to": [random.choice([None]+list(range(1,5))) for _ in range(n_emp)],
    }), SQL_DIR, "nw_employees")

    save(pl.DataFrame({"shipper_id": [1,2,3], "company_name": ["Speedy Express","United Package","Federal Shipping"],
        "phone": [rand_phone() for _ in range(3)]}), SQL_DIR, "nw_shippers")
    save(pl.DataFrame({"region_id": [1,2,3,4], "region_description": ["Eastern","Western","Northern","Southern"]}), SQL_DIR, "nw_regions")
    terr = ["New York","Boston","Philadelphia","Washington","Atlanta","Chicago","Detroit","Minneapolis",
            "Denver","Seattle","San Francisco","Los Angeles","Houston","Dallas","London","Manchester","Edinburgh","Paris"]
    save(pl.DataFrame({"territory_id": [f"T{str(i).zfill(3)}" for i in range(1, len(terr)+1)],
        "territory_description": terr, "region_id": random.choices([1,2,3,4], k=len(terr))}), SQL_DIR, "nw_territories")

    n_ord = 10000
    save(pl.DataFrame({
        "order_id": list(range(10248, 10248+n_ord)),
        "customer_id": random.choices(cids, k=n_ord),
        "employee_id": random.choices(range(1, n_emp+1), k=n_ord),
        "order_date": [rand_date(datetime(2021,1,1), datetime(2024,12,31)) for _ in range(n_ord)],
        "required_date": [rand_date(datetime(2021,1,15), datetime(2025,1,15)) for _ in range(n_ord)],
        "shipped_date": [rand_date(datetime(2021,1,5), datetime(2025,1,5)) if random.random()>0.05 else None for _ in range(n_ord)],
        "shipper_id": random.choices([1,2,3], k=n_ord),
        "freight": [round(random.uniform(1, 300), 2) for _ in range(n_ord)],
    }), SQL_DIR, "nw_orders")

    details = []
    for oid in range(10248, 10248+n_ord):
        for pid in random.sample(range(1, n_prod+1), random.randint(1, 5)):
            details.append({"order_id": oid, "product_id": pid, "unit_price": round(random.uniform(5,200), 2),
                "quantity": random.randint(1, 50), "discount": random.choice([0,0,0,0.05,0.1,0.15,0.2,0.25])})
    save(pl.DataFrame(details), SQL_DIR, "nw_order_details")

    # ═══ CHINOOK ═══
    n_artists = 500
    save(pl.DataFrame({"artist_id": list(range(1, n_artists+1)), "name": [f"Artist_{i}" for i in range(1, n_artists+1)]}), SQL_DIR, "ck_artists")
    n_albums = 1000
    save(pl.DataFrame({"album_id": list(range(1, n_albums+1)), "title": [f"Album_{i}" for i in range(1, n_albums+1)],
        "artist_id": random.choices(range(1, n_artists+1), k=n_albums)}), SQL_DIR, "ck_albums")
    mt = ["MPEG audio file","Protected AAC","Protected MPEG-4","Purchased AAC","AAC audio file"]
    save(pl.DataFrame({"media_type_id": list(range(1, len(mt)+1)), "name": mt}), SQL_DIR, "ck_media_types")
    genres = ["Rock","Jazz","Metal","Alternative","Classical","Blues","Latin","R&B/Soul","Reggae","Pop","Soundtrack","Hip Hop/Rap","Electronica","Country","Comedy","World"]
    save(pl.DataFrame({"genre_id": list(range(1, len(genres)+1)), "name": genres}), SQL_DIR, "ck_genres")

    n_tracks = 10000
    save(pl.DataFrame({
        "track_id": list(range(1, n_tracks+1)),
        "name": [f"Track_{i}" for i in range(1, n_tracks+1)],
        "album_id": random.choices(range(1, n_albums+1), k=n_tracks),
        "media_type_id": random.choices(range(1, len(mt)+1), weights=[0.5,0.15,0.05,0.2,0.1], k=n_tracks),
        "genre_id": random.choices(range(1, len(genres)+1), k=n_tracks),
        "composer": [f"Composer_{random.randint(1,100)}" if random.random()>0.2 else None for _ in range(n_tracks)],
        "milliseconds": [random.randint(60000, 600000) for _ in range(n_tracks)],
        "bytes": [random.randint(1000000, 50000000) for _ in range(n_tracks)],
        "unit_price": random.choices([0.99, 1.99], weights=[0.8,0.2], k=n_tracks),
    }), SQL_DIR, "ck_tracks")

    pl_names = ["Music","Movies","TV Shows","Audiobooks","90s Music","Classical","Heavy Metal","Brazilian Music","Grunge","On-The-Go","Jazz","Rock","Pop"]
    save(pl.DataFrame({"playlist_id": list(range(1, len(pl_names)+1)), "name": pl_names}), SQL_DIR, "ck_playlists")
    pt = []
    for plid in range(1, len(pl_names)+1):
        for tid in random.sample(range(1, n_tracks+1), random.randint(20, 200)):
            pt.append({"playlist_id": plid, "track_id": tid})
    save(pl.DataFrame(pt), SQL_DIR, "ck_playlist_track")

    n_ck_emp = 8
    save(pl.DataFrame({
        "employee_id": list(range(1, n_ck_emp+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_ck_emp)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_ck_emp)],
        "title": ["General Manager","Sales Manager","IT Manager","Sales Support Agent","Sales Support Agent","Sales Support Agent","IT Staff","IT Staff"],
        "reports_to": [None,1,1,2,2,2,3,3],
        "birth_date": [rand_date(datetime(1960,1,1), datetime(1990,12,31)) for _ in range(n_ck_emp)],
        "hire_date": [rand_date(datetime(2015,1,1), datetime(2023,6,30)) for _ in range(n_ck_emp)],
        "city": random.choices(["Calgary","Edmonton","Lethbridge"], k=n_ck_emp),
        "country": ["Canada"]*n_ck_emp,
        "email": [f"emp{i}@chinookcorp.com" for i in range(1, n_ck_emp+1)],
    }), SQL_DIR, "ck_employees")

    n_ck_cust = 200
    ccc2 = [rand_country_city() for _ in range(n_ck_cust)]
    save(pl.DataFrame({
        "customer_id": list(range(1, n_ck_cust+1)),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_ck_cust)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_ck_cust)],
        "company": [f"Company_{random.randint(1,50)}" if random.random()>0.5 else None for _ in range(n_ck_cust)],
        "email": [f"cust{i}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}" for i in range(1, n_ck_cust+1)],
        "city": [c[1] for c in ccc2], "state": [f"State_{random.randint(1,20)}" if random.random()>0.6 else None for _ in range(n_ck_cust)],
        "country": [c[0] for c in ccc2],
        "support_rep_id": random.choices([4,5,6], k=n_ck_cust),
    }), SQL_DIR, "ck_customers")

    n_inv = 3000
    save(pl.DataFrame({
        "invoice_id": list(range(1, n_inv+1)),
        "customer_id": random.choices(range(1, n_ck_cust+1), k=n_inv),
        "invoice_date": [rand_date(datetime(2020,1,1), datetime(2024,12,31)) for _ in range(n_inv)],
        "billing_city": [random.choice(CITIES_MAP[random.choice(COUNTRIES_EU)]) for _ in range(n_inv)],
        "billing_country": random.choices(COUNTRIES_EU, k=n_inv),
        "total": [round(random.uniform(0.99, 25.0), 2) for _ in range(n_inv)],
    }), SQL_DIR, "ck_invoices")

    il, ilid = [], 1
    for inv_id in range(1, n_inv+1):
        for tid in random.sample(range(1, n_tracks+1), random.randint(1, 10)):
            il.append({"invoice_line_id": ilid, "invoice_id": inv_id, "track_id": tid,
                "unit_price": random.choice([0.99, 1.99]), "quantity": 1}); ilid += 1
    save(pl.DataFrame(il), SQL_DIR, "ck_invoice_lines")

    # ═══ SAKILA ═══
    n_actors = 500
    save(pl.DataFrame({"actor_id": list(range(1, n_actors+1)),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_actors)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_actors)]}), SQL_DIR, "sk_actors")

    sk_cats = ["Action","Animation","Children","Classics","Comedy","Documentary","Drama","Family","Foreign","Games","Horror","Music","New","Sci-Fi","Sports","Travel"]
    save(pl.DataFrame({"category_id": list(range(1, len(sk_cats)+1)), "name": sk_cats}), SQL_DIR, "sk_categories")
    langs = ["English","Italian","Japanese","Mandarin","French","German"]
    save(pl.DataFrame({"language_id": list(range(1, len(langs)+1)), "name": langs}), SQL_DIR, "sk_languages")

    n_films = 5000
    ratings = ["G","PG","PG-13","R","NC-17"]
    sf_opts = ["Trailers","Commentaries","Deleted Scenes","Behind the Scenes"]
    save(pl.DataFrame({
        "film_id": list(range(1, n_films+1)),
        "title": [f"Film_{i}" for i in range(1, n_films+1)],
        "description": [f"A story about {random.choice(['adventure','love','mystery','action','comedy'])} in {random.choice(['a city','the jungle','space','a school','an island'])}" for _ in range(n_films)],
        "release_year": random.choices(range(2000, 2025), k=n_films),
        "language_id": random.choices(range(1, len(langs)+1), weights=[0.6,0.1,0.05,0.05,0.1,0.1], k=n_films),
        "rental_duration": random.choices([3,4,5,6,7], k=n_films),
        "rental_rate": random.choices([0.99, 2.99, 4.99], weights=[0.3,0.5,0.2], k=n_films),
        "length_min": [random.randint(46, 185) for _ in range(n_films)],
        "replacement_cost": [round(random.uniform(9.99, 29.99), 2) for _ in range(n_films)],
        "rating": random.choices(ratings, k=n_films),
        "special_features": [",".join(random.sample(sf_opts, random.randint(1,3))) if random.random()>0.1 else None for _ in range(n_films)],
    }), SQL_DIR, "sk_films")

    fa = []
    for fid in range(1, n_films+1):
        for aid in random.sample(range(1, n_actors+1), random.randint(1, 8)):
            fa.append({"actor_id": aid, "film_id": fid})
    save(pl.DataFrame(fa), SQL_DIR, "sk_film_actor")
    save(pl.DataFrame([{"film_id": fid, "category_id": random.randint(1, len(sk_cats))} for fid in range(1, n_films+1)]), SQL_DIR, "sk_film_category")

    country_list = COUNTRIES_EU + ["USA","Canada","Brazil","Australia","Japan","India"]
    save(pl.DataFrame({"country_id": list(range(1, len(country_list)+1)), "country": country_list}), SQL_DIR, "sk_countries")
    all_cities = []
    cid = 1
    for cidx, ctry in enumerate(country_list, 1):
        for cn in CITIES_MAP.get(ctry, [f"{ctry}_City_{i}" for i in range(1,4)]):
            all_cities.append({"city_id": cid, "city": cn, "country_id": cidx}); cid += 1
    save(pl.DataFrame(all_cities), SQL_DIR, "sk_cities")
    n_cities = len(all_cities)

    n_addr = 2000
    save(pl.DataFrame({
        "address_id": list(range(1, n_addr+1)),
        "address": [f"{random.randint(1,999)} {random.choice(['Main St','Oak Ave','Park Blvd','Elm St','Maple Dr'])}" for _ in range(n_addr)],
        "district": [f"District_{random.randint(1,30)}" for _ in range(n_addr)],
        "city_id": random.choices(range(1, n_cities+1), k=n_addr),
        "postal_code": [f"{random.randint(10000,99999)}" if random.random()>0.1 else None for _ in range(n_addr)],
        "phone": [rand_phone() for _ in range(n_addr)],
    }), SQL_DIR, "sk_addresses")

    save(pl.DataFrame({"store_id": [1,2], "manager_staff_id": [1,2], "address_id": [1,2]}), SQL_DIR, "sk_stores")
    save(pl.DataFrame({"staff_id": [1,2,3,4], "first_name": [random.choice(FIRST_NAMES) for _ in range(4)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(4)],
        "address_id": random.sample(range(1,10),4), "email": [f"staff{i}@sakilastore.com" for i in range(1,5)],
        "store_id": [1,2,1,2], "active": [True,True,True,False]}), SQL_DIR, "sk_staff")

    n_sk_cust = 2000
    save(pl.DataFrame({
        "customer_id": list(range(1, n_sk_cust+1)),
        "store_id": random.choices([1,2], k=n_sk_cust),
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_sk_cust)],
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_sk_cust)],
        "email": [f"cust{i}@sakilamail.com" for i in range(1, n_sk_cust+1)],
        "address_id": random.choices(range(1, n_addr+1), k=n_sk_cust),
        "active": random.choices([True, False], weights=[0.95,0.05], k=n_sk_cust),
        "create_date": [rand_date(datetime(2020,1,1), datetime(2024,6,30)) for _ in range(n_sk_cust)],
    }), SQL_DIR, "sk_customers")

    n_inv_sk = 20000
    save(pl.DataFrame({
        "inventory_id": list(range(1, n_inv_sk+1)),
        "film_id": random.choices(range(1, n_films+1), k=n_inv_sk),
        "store_id": random.choices([1,2], k=n_inv_sk),
    }), SQL_DIR, "sk_inventory")

    n_rent = 50000
    save(pl.DataFrame({
        "rental_id": list(range(1, n_rent+1)),
        "rental_date": [rand_datetime(datetime(2022,1,1), datetime(2024,12,31)) for _ in range(n_rent)],
        "inventory_id": random.choices(range(1, n_inv_sk+1), k=n_rent),
        "customer_id": random.choices(range(1, n_sk_cust+1), k=n_rent),
        "return_date": [rand_datetime(datetime(2022,1,3), datetime(2025,1,15)) if random.random()>0.03 else None for _ in range(n_rent)],
        "staff_id": random.choices([1,2,3,4], k=n_rent),
    }), SQL_DIR, "sk_rentals")

    n_pay = 50000
    save(pl.DataFrame({
        "payment_id": list(range(1, n_pay+1)),
        "customer_id": random.choices(range(1, n_sk_cust+1), k=n_pay),
        "staff_id": random.choices([1,2,3,4], k=n_pay),
        "rental_id": random.choices(range(1, n_rent+1), k=n_pay),
        "amount": [round(random.choice([0.99,2.99,4.99,5.99,7.99,9.99,11.99]),2) for _ in range(n_pay)],
        "payment_date": [rand_datetime(datetime(2022,1,1), datetime(2024,12,31)) for _ in range(n_pay)],
    }), SQL_DIR, "sk_payments")

    # ═══ TPC-H ═══
    region_names = ["AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"]
    save(pl.DataFrame({"r_regionkey": list(range(len(region_names))), "r_name": region_names,
        "r_comment": [f"Region comment {i}" for i in range(len(region_names))]}), SQL_DIR, "tpch_region")

    nations = [("ALGERIA",0),("ARGENTINA",1),("BRAZIL",1),("CANADA",1),("EGYPT",0),("ETHIOPIA",0),
        ("FRANCE",3),("GERMANY",3),("INDIA",2),("INDONESIA",2),("IRAN",4),("IRAQ",4),("JAPAN",2),
        ("JORDAN",4),("KENYA",0),("MOROCCO",0),("MOZAMBIQUE",0),("PERU",1),("CHINA",2),("ROMANIA",3),
        ("SAUDI ARABIA",4),("VIETNAM",2),("RUSSIA",3),("UNITED KINGDOM",3),("UNITED STATES",1)]
    save(pl.DataFrame({"n_nationkey": list(range(len(nations))), "n_name": [n[0] for n in nations],
        "n_regionkey": [n[1] for n in nations], "n_comment": [f"Comment {i}" for i in range(len(nations))]}), SQL_DIR, "tpch_nation")

    n_sup = 500
    save(pl.DataFrame({
        "s_suppkey": list(range(1, n_sup+1)),
        "s_name": [f"Supplier#{str(i).zfill(9)}" for i in range(1, n_sup+1)],
        "s_address": [f"{random.randint(1,999)} Street_{random.randint(1,50)}" for _ in range(n_sup)],
        "s_nationkey": random.choices(range(len(nations)), k=n_sup),
        "s_phone": [f"{random.randint(10,34)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(n_sup)],
        "s_acctbal": [round(random.uniform(-999, 9999), 2) for _ in range(n_sup)],
        "s_comment": [f"Supp comment {i}" for i in range(1, n_sup+1)],
    }), SQL_DIR, "tpch_supplier")

    n_parts = 2000
    mfgrs = [f"Manufacturer#{i}" for i in range(1,6)]
    brands = [f"Brand#{i}{j}" for i in range(1,6) for j in range(1,6)]
    tp = ["STANDARD","SMALL","MEDIUM","LARGE","ECONOMY","PROMO"]
    tm = ["POLISHED","BRUSHED","BURNISHED","PLATED","ANODIZED"]
    ts = ["TIN","NICKEL","BRASS","STEEL","COPPER"]
    containers = ["SM CASE","SM BOX","SM PACK","SM PKG","MED BAG","MED BOX","MED PKG","LG CASE","LG BOX","LG PACK","LG DRUM","WRAP CASE"]
    save(pl.DataFrame({
        "p_partkey": list(range(1, n_parts+1)),
        "p_name": [f"Part_{i}" for i in range(1, n_parts+1)],
        "p_mfgr": random.choices(mfgrs, k=n_parts),
        "p_brand": random.choices(brands, k=n_parts),
        "p_type": [f"{random.choice(tp)} {random.choice(tm)} {random.choice(ts)}" for _ in range(n_parts)],
        "p_size": [random.randint(1,50) for _ in range(n_parts)],
        "p_container": random.choices(containers, k=n_parts),
        "p_retailprice": [round(random.uniform(900, 2100), 2) for _ in range(n_parts)],
        "p_comment": [f"Part comment {i}" for i in range(1, n_parts+1)],
    }), SQL_DIR, "tpch_part")

    ps = []
    for pk in range(1, n_parts+1):
        for sk in random.sample(range(1, n_sup+1), 4):
            ps.append({"ps_partkey": pk, "ps_suppkey": sk, "ps_availqty": random.randint(1,9999),
                "ps_supplycost": round(random.uniform(1,1000), 2), "ps_comment": f"PS {pk}-{sk}"})
    save(pl.DataFrame(ps), SQL_DIR, "tpch_partsupp")

    n_tc = 3000
    mkts = ["AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"]
    save(pl.DataFrame({
        "c_custkey": list(range(1, n_tc+1)),
        "c_name": [f"Customer#{str(i).zfill(9)}" for i in range(1, n_tc+1)],
        "c_address": [f"{random.randint(1,9999)} Addr_{random.randint(1,200)}" for _ in range(n_tc)],
        "c_nationkey": random.choices(range(len(nations)), k=n_tc),
        "c_phone": [f"{random.randint(10,34)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(n_tc)],
        "c_acctbal": [round(random.uniform(-999, 9999), 2) for _ in range(n_tc)],
        "c_mktsegment": random.choices(mkts, k=n_tc),
        "c_comment": [f"Cust comment {i}" for i in range(1, n_tc+1)],
    }), SQL_DIR, "tpch_customer")

    n_to = 15000
    prios = ["1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"]
    save(pl.DataFrame({
        "o_orderkey": list(range(1, n_to+1)),
        "o_custkey": random.choices(range(1, n_tc+1), k=n_to),
        "o_orderstatus": random.choices(["O","F","P"], weights=[0.25,0.5,0.25], k=n_to),
        "o_totalprice": [round(random.uniform(500, 500000), 2) for _ in range(n_to)],
        "o_orderdate": [rand_date(datetime(2020,1,1), datetime(2024,12,31)) for _ in range(n_to)],
        "o_orderpriority": random.choices(prios, k=n_to),
        "o_clerk": [f"Clerk#{str(random.randint(1,1000)).zfill(9)}" for _ in range(n_to)],
        "o_shippriority": [0]*n_to,
        "o_comment": [f"Order comment {i}" for i in range(1, n_to+1)],
    }), SQL_DIR, "tpch_orders")

    li = []
    for ok in range(1, n_to+1):
        for ln in range(1, random.randint(2, 7)+1):
            qty = round(random.uniform(1, 50), 2)
            price = round(random.uniform(9, 1000), 2)
            li.append({"l_orderkey": ok, "l_partkey": random.randint(1,n_parts), "l_suppkey": random.randint(1,n_sup),
                "l_linenumber": ln, "l_quantity": qty, "l_extendedprice": round(qty*price, 2),
                "l_discount": round(random.uniform(0,0.10), 2), "l_tax": round(random.uniform(0,0.08), 2),
                "l_returnflag": random.choice(["R","A","N"]), "l_linestatus": random.choice(["O","F"]),
                "l_shipdate": rand_date(datetime(2020,1,15), datetime(2025,6,30)),
                "l_commitdate": rand_date(datetime(2020,1,10), datetime(2025,6,25)),
                "l_receiptdate": rand_date(datetime(2020,1,20), datetime(2025,7,10)),
                "l_shipinstruct": random.choice(["DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"]),
                "l_shipmode": random.choice(["REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"]),
                "l_comment": f"LI {ok}-{ln}"})
    save(pl.DataFrame(li), SQL_DIR, "tpch_lineitem")

    # ═══ SPIDER MINI ═══
    n_sing = 50
    save(pl.DataFrame({"singer_id": list(range(1, n_sing+1)), "name": [rand_name() for _ in range(n_sing)],
        "country": random.choices(COUNTRIES_EU, k=n_sing),
        "song_name": [f"Song_{i}" for i in range(1, n_sing+1)],
        "song_release_year": [str(random.randint(1990, 2024)) for _ in range(n_sing)],
        "age": [random.randint(20,65) for _ in range(n_sing)],
        "is_male": random.choices([True, False], k=n_sing)}), SQL_DIR, "sp_singer")

    n_stad = 25
    save(pl.DataFrame({"stadium_id": list(range(1, n_stad+1)),
        "location": [random.choice(CITIES_MAP[random.choice(COUNTRIES_EU)]) for _ in range(n_stad)],
        "name": [f"Stadium_{i}" for i in range(1, n_stad+1)],
        "capacity": [random.randint(5000,80000) for _ in range(n_stad)],
        "average_attendance": [random.randint(3000,60000) for _ in range(n_stad)]}), SQL_DIR, "sp_stadium")

    n_conc = 100
    save(pl.DataFrame({"concert_id": list(range(1, n_conc+1)),
        "concert_name": [f"Concert_{i}" for i in range(1, n_conc+1)],
        "theme": random.choices(["Autumn","Free","Wild","Summer","Rock","Jazz","Pop","Classical"], k=n_conc),
        "stadium_id": random.choices(range(1, n_stad+1), k=n_conc),
        "year": [str(random.randint(2020, 2025)) for _ in range(n_conc)]}), SQL_DIR, "sp_concert")

    perf = []
    for cid in range(1, n_conc+1):
        for sid in random.sample(range(1, n_sing+1), random.randint(1, 8)):
            perf.append({"singer_id": sid, "concert_id": cid})
    save(pl.DataFrame(perf), SQL_DIR, "sp_performance")

    # World
    wc = [("FRA","France","Europe","Western Europe",551695,67390000,82.5,2583560,"Republic"),
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
        ("THA","Thailand","Asia","Southeast Asia",513120,69800000,76.9,543650,"Constitutional Monarchy")]
    save(pl.DataFrame({"code": [w[0] for w in wc], "name": [w[1] for w in wc], "continent": [w[2] for w in wc],
        "region": [w[3] for w in wc], "surface_area": [float(w[4]) for w in wc], "population": [w[5] for w in wc],
        "life_expectancy": [w[6] for w in wc], "gnp": [float(w[7]) for w in wc],
        "government_form": [w[8] for w in wc], "head_of_state": [f"Leader_{w[0]}" for w in wc]}), SQL_DIR, "sp_world_country")

    wcit = []
    wcid = 1
    for w in wc:
        for i in range(random.randint(5, 15)):
            wcit.append({"city_id": wcid, "name": f"{w[1]}_City_{i+1}", "country_code": w[0],
                "district": f"District_{random.randint(1,10)}", "population": random.randint(50000, 15000000)}); wcid += 1
    save(pl.DataFrame(wcit), SQL_DIR, "sp_world_city")

    wlang = []
    all_langs = ["English","French","Spanish","German","Arabic","Chinese","Hindi","Portuguese","Japanese","Korean","Russian","Thai","Italian","Dutch"]
    for w in wc:
        rem = 100.0
        for i, l in enumerate(random.sample(all_langs, random.randint(2, 5))):
            pct = round(rem * random.uniform(0.3, 0.9), 1) if i < 3 else round(rem, 1)
            rem -= pct
            wlang.append({"country_code": w[0], "language": l, "is_official": i == 0, "percentage": pct})
    save(pl.DataFrame(wlang), SQL_DIR, "sp_world_language")

    # Pets
    n_stu = 80
    save(pl.DataFrame({"stu_id": list(range(1, n_stu+1)),
        "last_name": [random.choice(LAST_NAMES) for _ in range(n_stu)],
        "first_name": [random.choice(FIRST_NAMES) for _ in range(n_stu)],
        "age": [random.randint(18, 28) for _ in range(n_stu)],
        "sex": random.choices(["M","F"], k=n_stu),
        "major": random.choices([600,520,540,100,200,300], k=n_stu),
        "advisor": random.choices(range(1101, 1120), k=n_stu),
        "city_code": random.choices(["NYC","CHI","LAX","HOU","PHX","SAN"], k=n_stu)}), SQL_DIR, "sp_student")

    n_pet = 50
    save(pl.DataFrame({"pet_id": list(range(1, n_pet+1)),
        "pet_type": random.choices(["cat","dog","hamster","fish","parrot","rabbit"], k=n_pet),
        "pet_age": [random.randint(1, 15) for _ in range(n_pet)],
        "weight": [round(random.uniform(0.5, 40), 1) for _ in range(n_pet)]}), SQL_DIR, "sp_pet")

    hp = []
    for sid in range(1, n_stu+1):
        if random.random() > 0.3:
            for pid in random.sample(range(1, n_pet+1), random.randint(1, 3)):
                hp.append({"stu_id": sid, "pet_id": pid})
    save(pl.DataFrame(hp), SQL_DIR, "sp_has_pet")

    # Cars
    makers = [("Toyota","Toyota Motor Corporation","Japan"),("Ford","Ford Motor Company","USA"),
        ("BMW","Bayerische Motoren Werke","Germany"),("Honda","Honda Motor Co.","Japan"),
        ("Volkswagen","Volkswagen AG","Germany"),("Fiat","Fiat Chrysler","Italy"),
        ("Peugeot","Stellantis","France"),("Hyundai","Hyundai Motor","South Korea")]
    save(pl.DataFrame({"id": list(range(1, len(makers)+1)), "maker": [m[0] for m in makers],
        "full_name": [m[1] for m in makers], "country": [m[2] for m in makers]}), SQL_DIR, "sp_car_maker")

    n_cars = 500
    save(pl.DataFrame({
        "car_id": list(range(1, n_cars+1)),
        "maker_id": random.choices(range(1, len(makers)+1), k=n_cars),
        "model": [f"Model_{i}" for i in range(1, n_cars+1)],
        "mpg": [round(random.uniform(12, 50), 1) for _ in range(n_cars)],
        "cylinders": random.choices([4,4,4,6,6,8], k=n_cars),
        "horsepower": [round(random.uniform(60, 350), 0) if random.random()>0.05 else None for _ in range(n_cars)],
        "weight": [random.randint(1800, 5000) for _ in range(n_cars)],
        "accelerate": [round(random.uniform(8, 22), 1) for _ in range(n_cars)],
        "year": random.choices(range(2010, 2025), k=n_cars),
    }), SQL_DIR, "sp_car_data")

    # Flights
    airports = [("Charles de Gaulle","Paris","France","CDG","LFPG",392),("Heathrow","London","UK","LHR","EGLL",83),
        ("Frankfurt","Frankfurt","Germany","FRA","EDDF",364),("Schiphol","Amsterdam","Netherlands","AMS","EHAM",-11),
        ("Barajas","Madrid","Spain","MAD","LEMD",2000),("JFK","New York","USA","JFK","KJFK",13),
        ("LAX","Los Angeles","USA","LAX","KLAX",126),("Narita","Tokyo","Japan","NRT","RJAA",141),
        ("Sydney","Sydney","Australia","SYD","YSSY",21),("Dubai","Dubai","UAE","DXB","OMDB",62),
        ("O'Hare","Chicago","USA","ORD","KORD",672),("Changi","Singapore","Singapore","SIN","WSSS",22)]
    save(pl.DataFrame({"airport_id": list(range(1, len(airports)+1)),
        "airport_name": [a[0] for a in airports], "city": [a[1] for a in airports],
        "country": [a[2] for a in airports], "iata": [a[3] for a in airports],
        "icao": [a[4] for a in airports], "altitude": [a[5] for a in airports]}), SQL_DIR, "sp_airport")

    airlines = [("Air France","AF","AFR","France",True),("Lufthansa","LH","DLH","Germany",True),
        ("British Airways","BA","BAW","UK",True),("KLM","KL","KLM","Netherlands",True),
        ("Iberia","IB","IBE","Spain",True),("Ryanair","FR","RYR","Ireland",True),
        ("EasyJet","U2","EZY","UK",True),("Emirates","EK","UAE","UAE",True),
        ("Delta","DL","DAL","USA",True),("United","UA","UAL","USA",True),
        ("ANA","NH","ANA","Japan",True),("Qantas","QF","QFA","Australia",True),
        ("OldAir",None,None,"France",False)]
    save(pl.DataFrame({"airline_id": list(range(1, len(airlines)+1)), "name": [a[0] for a in airlines],
        "iata": [a[1] for a in airlines], "icao": [a[2] for a in airlines],
        "country": [a[3] for a in airlines], "active": [a[4] for a in airlines]}), SQL_DIR, "sp_airline")

    n_routes = 1000
    save(pl.DataFrame({"route_id": list(range(1, n_routes+1)),
        "airline_id": random.choices(range(1, len(airlines)+1), k=n_routes),
        "src_airport_id": random.choices(range(1, len(airports)+1), k=n_routes),
        "dst_airport_id": random.choices(range(1, len(airports)+1), k=n_routes),
        "codeshare": random.choices([True, False], weights=[0.2,0.8], k=n_routes),
        "stops": random.choices([0,0,0,0,1,1,2], k=n_routes)}), SQL_DIR, "sp_route")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("POLARS BENCHMARK — DATASET GENERATOR (LARGE)")
    print("=" * 60)

    gen_sql()

    # Summary
    total_rows = 0
    total_tables = 0
    for d in [SQL_DIR]:
        for f in d.glob("*.parquet"):
            total_rows += pl.read_parquet(f).height
            total_tables += 1

    print(f"\n{'='*60}")
    print(f"DONE — {total_tables} tables, {total_rows:,} total rows")
    print(f"Output: {SQL_DIR}/")

if __name__ == "__main__":
    main()
