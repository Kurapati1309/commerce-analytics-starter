
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import os, argparse, random

# ------------------------------
# Configuration
# ------------------------------
SEGMENTS = ["men", "women", "kids", "unisex"]
CATEGORIES = {
    "tops": {
        "styles": ["tee", "tank", "crop", "hoodie", "crewneck"],
        "price_range": (18, 120),
        "cost_range": (6, 40),
        "size_map": {
            "men": ["S","M","L","XL","XXL"],
            "women": ["XS","S","M","L","XL"],
            "kids": ["2T","3T","4T","5","6","7","8","10","12"],
            "unisex": ["XS","S","M","L","XL","XXL"]
        }
    },
    "bottoms": {
        "styles": ["leggings", "joggers", "shorts", "pants"],
        "price_range": (22, 140),
        "cost_range": (8, 55),
        "size_map": {
            "men": ["S","M","L","XL","XXL"],
            "women": ["XS","S","M","L","XL"],
            "kids": ["2T","3T","4T","5","6","7","8","10","12"],
            "unisex": ["XS","S","M","L","XL","XXL"]
        }
    },
    "loungewear": {
        "styles": ["sweatshirt", "lounge_pants", "robe", "set"],
        "price_range": (30, 160),
        "cost_range": (12, 70),
        "size_map": {
            "men": ["S","M","L","XL","XXL"],
            "women": ["XS","S","M","L","XL"],
            "kids": ["XS","S","M","L"],
            "unisex": ["XS","S","M","L","XL","XXL"]
        }
    },
    "intimates": {
        "styles": ["bra", "bralette", "underwear", "shapewear"],
        "price_range": (20, 90),
        "cost_range": (8, 40),
        "size_map": {
            "men": ["S","M","L","XL"],
            "women": ["XS","S","M","L","XL","2XL"],
            "kids": ["XS","S","M","L"],
            "unisex": ["XS","S","M","L","XL"]
        }
    },
    "accessories": {
        "styles": ["socks", "headband", "bag", "cap"],
        "price_range": (8, 60),
        "cost_range": (3, 20),
        "size_map": {
            "men": ["OS"],
            "women": ["OS"],
            "kids": ["OS"],
            "unisex": ["OS"]
        }
    },
    "seasonal": {
        "styles": ["collab_top", "collab_bottom", "limited_set"],
        "price_range": (35, 200),
        "cost_range": (15, 100),
        "size_map": {
            "men": ["S","M","L","XL","XXL"],
            "women": ["XS","S","M","L","XL"],
            "kids": ["XS","S","M","L"],
            "unisex": ["XS","S","M","L","XL","XXL"]
        }
    }
}

COLORS = ["black","white","heather_gray","navy","olive","sand","blush","maroon","cobalt","sage","charcoal"]
WAREHOUSES = ["WH1", "WH2", "WH3"]

CHANNELS = ["google","facebook","tiktok","email","affiliate"]
DEVICES = ["mobile","desktop"]

# segment weights for order probability
SEGMENT_WEIGHTS = {
    "women": 0.38,
    "men": 0.32,
    "kids": 0.15,
    "unisex": 0.15
}

# category weights (among chosen segment)
CATEGORY_WEIGHTS = {
    "tops": 0.28,
    "bottoms": 0.26,
    "loungewear": 0.18,
    "intimates": 0.14,
    "accessories": 0.08,
    "seasonal": 0.06
}



def ensure(p):
    os.makedirs(p, exist_ok=True)


def bounded_random_int(low, high):
    return int(random.uniform(low, high))


def pick_weighted(dct):
    items = list(dct.items())
    keys, weights = zip(*items)
    return random.choices(keys, weights=weights, k=1)[0]


def build_catalog(target_skus:int=380, rng_seed:int=42):
    random.seed(rng_seed)
    np.random.seed(rng_seed)

    rows = []
    sku_counter = 1

    # distribute SKUs across segments & categories roughly per weights
    # we'll iterate combinations and stop when reaching target
    while len(rows) < target_skus:
        segment = pick_weighted(SEGMENT_WEIGHTS)
        category = pick_weighted(CATEGORY_WEIGHTS)
        meta = CATEGORIES[category]

        style = random.choice(meta["styles"])
        size = random.choice(meta["size_map"][segment])
        color = random.choice(COLORS)

        # price and cost
        pmin, pmax = meta["price_range"]
        cmin, cmax = meta["cost_range"]
        price = round(random.uniform(pmin, pmax), 2)
        cost = round(min(price * random.uniform(0.3, 0.6), random.uniform(cmin, cmax)), 2)

        # title
        title = f"{segment.capitalize()} {category[:-1] if category.endswith('s') else category} - {style} - {color} - {size}"
        sku = f"SKU{sku_counter:05d}"

        # seasonal release dates in last 90 days
        release_dt = date.today() - timedelta(days=random.randint(0, 90))

        rows.append({
            "sku": sku,
            "segment": segment,
            "category": category,
            "style": style,
            "color": color,
            "size": size,
            "title": title,
            "cost": cost,
            "price": price,
            "release_date": release_dt.isoformat()
        })
        sku_counter += 1

    return pd.DataFrame(rows)


def generate_data(days:int=90, out_dir:str="scripts/data", target_skus:int=380, rng_seed:int=42):
    ensure(out_dir)

    catalog = build_catalog(target_skus=target_skus, rng_seed=rng_seed)

    # Save products catalog
    products_path = os.path.join(out_dir, "products_raw.csv")
    catalog[["sku","title","category","cost","price","segment","style","color","size","release_date"]].to_csv(products_path, index=False)

    # Inventory (latest snapshot per SKU x warehouse)
    inv_rows = []
    for _, row in catalog.iterrows():
        for wh in WAREHOUSES:
            on_hand = bounded_random_int(0, 600)
            reserved = bounded_random_int(0, int(on_hand*0.2))
            inv_rows.append({
                "sku": row["sku"],
                "warehouse": wh,
                "on_hand": on_hand,
                "reserved": reserved,
                "updated_ts": datetime.utcnow().isoformat()
            })
    pd.DataFrame(inv_rows).to_csv(os.path.join(out_dir, "inventory_raw.csv"), index=False)

    # Ad spend
    ad_rows = []
    today = date.today()
    for d in range(days):
        the_day = today - timedelta(days=d)
        for ch in CHANNELS:
            ad_rows.append({
                "date": the_day.isoformat(),
                "channel": ch,
                "campaign": f"{ch}_campaign_{random.randint(1,6)}",
                "spend": round(random.uniform(80, 800), 2),
                "clicks": random.randint(500, 15000),
                "impressions": random.randint(5000, 300000)
            })
    pd.DataFrame(ad_rows).to_csv(os.path.join(out_dir, "ad_spend_raw.csv"), index=False)

    # Events, orders, items, returns
    orders, items, events, returns = [], [], [], []
    users = [f"U{i:06d}" for i in range(1, 5001)]
    order_id = 1
    order_item_id = 1

    for d in range(days):
        day_dt = datetime.utcnow().date() - timedelta(days=d)
        # variable traffic
        sessions_today = random.randint(600, 2200)

        for _ in range(sessions_today):
            user = random.choice(users)
            session = f"S{random.randint(1, 10_000_000)}"
            ts_base = datetime.combine(day_dt, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))

            utm_source = random.choice(CHANNELS)
            utm_campaign = f"{utm_source}_campaign_{random.randint(1,6)}"
            device = random.choice(DEVICES)

            # event path with probabilities
            path = ["page_view"]
            if random.random() < 0.65: path.append("add_to_cart")
            if random.random() < 0.45: path.append("checkout")
            if random.random() < 0.28: path.append("purchase")

            for et in path:
                events.append({
                    "event_id": f"E{random.randint(1, 999_999_999)}",
                    "user_id": user,
                    "session_id": session,
                    "event_ts": (ts_base + timedelta(seconds=random.randint(0, 600))).isoformat(),
                    "event_type": et,
                    "device": device,
                    "page_url": "https://aurorawear.example/product",
                    "utm_source": utm_source,
                    "utm_campaign": utm_campaign
                })

            if "purchase" in path:
                # choose a segment → category to bias SKU selection
                seg = pick_weighted(SEGMENT_WEIGHTS)
                # filter catalog by segment; further bias categories
                cat = pick_weighted(CATEGORY_WEIGHTS)
                seg_cat = catalog[(catalog.segment == seg) & (catalog.category == cat)]
                if seg_cat.empty:
                    seg_cat = catalog[catalog.segment == seg]
                # choose 1–4 items
                n_items = random.randint(1, 4)
                subtotal = 0.0
                for _ in range(n_items):
                    prod = seg_cat.sample(1).iloc[0]
                    qty = random.randint(1, 3)
                    price = float(prod["price"])
                    line_total = price * qty
                    items.append({
                        "order_item_id": order_item_id,
                        "order_id": order_id,
                        "sku": prod["sku"],
                        "qty": qty,
                        "unit_price": price,
                        "line_total": round(line_total, 2)
                    })
                    subtotal += line_total
                    order_item_id += 1

                discount = round(subtotal * random.uniform(0, 0.18), 2)
                tax = round(subtotal * 0.0825, 2)
                shipping = round(random.uniform(0, 12), 2)
                total = round(subtotal - discount + tax + shipping, 2)
                status = random.choices(["placed","fulfilled","refunded"], weights=[0.2,0.72,0.08])[0]
                coupon = random.choice(["", "SAVE10", "WELCOME", "VIP", "FREESHIP"])

                orders.append({
                    "order_id": order_id,
                    "user_id": user,
                    "order_ts": (ts_base + timedelta(seconds=random.randint(0, 3600))).isoformat(),
                    "subtotal": round(subtotal,2),
                    "discount": discount,
                    "tax": tax,
                    "shipping": shipping,
                    "total": total,
                    "status": status,
                    "coupon_code": coupon,
                    "utm_source": utm_source,
                    "utm_campaign": utm_campaign
                })

                # returns (~7% of fulfilled orders)
                if status == "fulfilled" and random.random() < 0.07:
                    returns.append({
                        "return_id": f"R{random.randint(1, 999_999_999)}",
                        "order_id": order_id,
                        "sku": prod["sku"],
                        "qty": 1,
                        "reason": random.choice(["size","defect","changed_mind"]),
                        "processed_ts": (ts_base + timedelta(days=random.randint(1,14))).isoformat(),
                        "amount_refunded": round(random.uniform(10, min(120, total)), 2)
                    })
                order_id += 1

    # write outputs
    out_dir = out_dir
    pd.DataFrame(orders).to_csv(os.path.join(out_dir, "orders_raw.csv"), index=False)
    pd.DataFrame(items).to_csv(os.path.join(out_dir, "order_items_raw.csv"), index=False)
    pd.DataFrame(events).to_csv(os.path.join(out_dir, "events_stream.csv"), index=False)
    pd.DataFrame(returns).to_csv(os.path.join(out_dir, "returns_raw.csv"), index=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="How many days of history to generate")
    ap.add_argument("--out_dir", type=str, default="scripts/data", help="Output directory for CSV files")
    ap.add_argument("--target_skus", type=int, default=380, help="Approx number of SKUs to generate (300–400 recommended)")
    args = ap.parse_args()
    generate_data(days=args.days, out_dir=args.out_dir, target_skus=args.target_skus)
