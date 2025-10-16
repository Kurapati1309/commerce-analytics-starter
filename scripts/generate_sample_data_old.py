import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os, argparse, random

def daterange(days):
    today = datetime.utcnow().date()
    for i in range(days):
        yield today - timedelta(days=i)

def ensure(p):
    os.makedirs(p, exist_ok=True)

def main(days=90, out_dir='scripts/data'):
    rng = np.random.default_rng(42)
    ensure(out_dir)

    # Products
    skus = [f"SKU{i:04d}" for i in range(1, 51)]
    products = pd.DataFrame({
        "sku": skus,
        "title": [f"Product {i}" for i in range(1, 51)],
        "category": rng.choice(["tops","bottoms","intimates","lounge"], 50),
        "cost": rng.integers(5, 30, 50),
        "price": rng.integers(20, 120, 50)
    })
    products.to_csv(os.path.join(out_dir, "products_raw.csv"), index=False)

    # Inventory snapshots (latest only for simplicity)
    inv = pd.DataFrame({
        "sku": np.repeat(skus, 2),
        "warehouse": ["WH1","WH2"] * len(skus),
        "on_hand": rng.integers(0, 500, len(skus)*2),
        "reserved": rng.integers(0, 50, len(skus)*2),
        "updated_ts": datetime.utcnow().isoformat()
    })
    inv.to_csv(os.path.join(out_dir, "inventory_raw.csv"), index=False)

    # Ad spend daily
    channels = ["google","facebook","tiktok","email","affiliate"]
    ad_rows = []
    for d in daterange(days):
        for c in channels:
            ad_rows.append({
                "date": d.isoformat(),
                "channel": c,
                "campaign": f"{c}_campaign_{random.randint(1,5)}",
                "spend": round(random.uniform(50, 500), 2),
                "clicks": random.randint(100, 5000),
                "impressions": random.randint(1000, 100000)
            })
    pd.DataFrame(ad_rows).to_csv(os.path.join(out_dir, "ad_spend_raw.csv"), index=False)

    # Orders + items + events + returns
    order_rows, item_rows, event_rows, return_rows = [], [], [], []

    users = [f"U{i:05d}" for i in range(1, 1501)]
    status_choices = ["placed","fulfilled","refunded"]
    event_types = ["page_view","add_to_cart","checkout","purchase"]

    order_id = 1
    order_item_id = 1
    for d in daterange(days):
        # sessions/events
        for _ in range(random.randint(200, 800)):
            user = random.choice(users)
            session = f"S{random.randint(1,999999)}"
            ts = datetime.combine(d, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
            # simple funnel simulation
            ev_path = ["page_view"]
            if random.random() < 0.6: ev_path.append("add_to_cart")
            if random.random() < 0.4: ev_path.append("checkout")
            if random.random() < 0.25: ev_path.append("purchase")

            utm_source = random.choice(channels)
            utm_campaign = f"{utm_source}_campaign_{random.randint(1,5)}"
            device = random.choice(["mobile","desktop"])

            for et in ev_path:
                event_rows.append({
                    "event_id": f"E{random.randint(1,99999999)}",
                    "user_id": user,
                    "session_id": session,
                    "event_ts": ts.isoformat(),
                    "event_type": et,
                    "device": device,
                    "page_url": "https://example.com/p",
                    "utm_source": utm_source,
                    "utm_campaign": utm_campaign
                })

            if "purchase" in ev_path:
                n_items = random.randint(1, 4)
                prices = []
                subtotal = 0.0
                for _ in range(n_items):
                    sku = random.choice(skus)
                    qty = random.randint(1, 3)
                    price = float(products.loc[products['sku']==sku, 'price'].iloc[0])
                    line_total = price * qty
                    item_rows.append({
                        "order_item_id": order_item_id,
                        "order_id": order_id,
                        "sku": sku,
                        "qty": qty,
                        "unit_price": price,
                        "line_total": line_total
                    })
                    subtotal += line_total
                    order_item_id += 1
                discount = round(subtotal * random.uniform(0, 0.15), 2)
                tax = round(subtotal * 0.0825, 2)
                shipping = round(random.uniform(0, 10), 2)
                total = round(subtotal - discount + tax + shipping, 2)
                status = random.choices(status_choices, weights=[0.2,0.7,0.1])[0]
                coupon = random.choice(["","SAVE10","WELCOME","VIP"])
                order_rows.append({
                    "order_id": order_id,
                    "user_id": user,
                    "order_ts": ts.isoformat(),
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
                # occasional return
                if status != "refunded" and random.random() < 0.08:
                    return_rows.append({
                        "return_id": f"R{random.randint(1,9999999)}",
                        "order_id": order_id,
                        "sku": random.choice(skus),
                        "qty": 1,
                        "reason": random.choice(["size","defect","changed_mind"]),
                        "processed_ts": (ts + timedelta(days=random.randint(1,14))).isoformat(),
                        "amount_refunded": round(random.uniform(10,100),2)
                    })
                order_id += 1

    pd.DataFrame(order_rows).to_csv(os.path.join(out_dir, "orders_raw.csv"), index=False)
    pd.DataFrame(item_rows).to_csv(os.path.join(out_dir, "order_items_raw.csv"), index=False)
    pd.DataFrame(event_rows).to_csv(os.path.join(out_dir, "events_stream.csv"), index=False)
    pd.DataFrame(return_rows).to_csv(os.path.join(out_dir, "returns_raw.csv"), index=False)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--out_dir", type=str, default="scripts/data")
    args = ap.parse_args()
    main(days=args.days, out_dir=args.out_dir)
