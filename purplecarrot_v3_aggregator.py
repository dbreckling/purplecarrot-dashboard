#!/usr/bin/env python3
"""Builds dashboard_v3_data.json — supplemental data on top of v2.

What v2 already has and we re-use untouched:
  - dailyData per-day rollups (with per-placement breakdown)
  - global totals, attribution, programmatic, channels, campaigns
  - converter details
  - allOrders (all purchase rows)

What v3 adds:
  - zipRollup: per-ZIP subscriber count, revenue, AOV, paid vs organic
  - geoRollup: per-state and per-city quick aggregates
  - skuTiers: subscription price-tier breakdown (small/medium/large/XL boxes
    inferred from the price field on the products array)
  - channelOverlap: solo-vs-multi-touch breakdown
  - topRecipes: extracted from product names if available (placeholder for
    when PC starts sending per-recipe rows)

Runtime: pulls all PC purchases from the API, computes aggregations, writes
the supplemental file. The v3 dashboard loads BOTH files at runtime.
"""
import base64
import json
import os
import re
import ssl
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime

import pgeocode  # offline ZIP -> (city, state, lat, lng) lookup

_NOMI = pgeocode.Nominatim("us")

URL = "https://dev.graph.script.flowershop.media/graphql"
AUTH = "Basic " + base64.b64encode(b"developer:FlowerShop2024DevGraphQL123").decode()
ADVERTISER_ID = "1060"
OUT_PATH = "/Users/davidbreckling/Desktop/Calude/Advertiser Dashbaords/PurpleCarrot/dashboard_v3_data.json"

# Price tier inference. PC sub prices we've seen: 12 (gift redeem),
# 39-40 (small / promotional), 60-80 (medium), 95-115 (standard 6/7 meal),
# 120+ (large box / family).
def tier_for_price(p):
    p = float(p or 0)
    if p == 0:        return "Gift / Redeem"
    if p < 35:        return "Trial / Promo"
    if p < 60:        return "Small Box (~$40-60)"
    if p < 85:        return "Medium Box (~$60-85)"
    if p < 115:       return "Standard Box (~$85-115)"
    return "Large Box ($115+)"


def gql(query, retries=3):
    req = urllib.request.Request(
        URL,
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", "Authorization": AUTH},
    )
    last_err = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(
                req, context=ssl.create_default_context(), timeout=180
            ) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
    raise last_err


def fetch_purchases(start_iso="2026-03-01"):
    print(f"Pulling purchases for adv {ADVERTISER_ID} since {start_iso}...")
    out = []
    offset = 0
    LIMIT = 5000
    while True:
        q = (
            '{ allPurchases(filter:{ advertiserId:{ equalTo:"%s" } '
            'time:{ greaterThanOrEqualTo:"%sT00:00:00Z" } } '
            'first:%d offset:%d orderBy:TIME_ASC){ totalCount nodes { '
            'time dedupeId revenue region city ip scriptId products data url } } }'
        ) % (ADVERTISER_ID, start_iso, LIMIT, offset)
        r = gql(q)
        nodes = r["data"]["allPurchases"]["nodes"]
        total = r["data"]["allPurchases"]["totalCount"]
        out.extend(nodes)
        print(f"  offset={offset}: {len(nodes)} (running={len(out)} / total={total})")
        offset += len(nodes)
        if not nodes or len(nodes) < LIMIT or offset >= total:
            break
    return out


def is_real_pc_order(p):
    d = str(p.get("dedupeId") or "")
    if not d.isdigit():
        return False
    try:
        n = int(d)
    except ValueError:
        return False
    return 10_000 <= n <= 3_999_999


def fetch_attributed_set():
    """Return set of dedupeIds that have an allPurchaseAttributions row."""
    attr = set()
    offset = 0
    while True:
        q = (
            '{ allPurchaseAttributions(filter:{ advertiserId:{ equalTo:"%s" } } '
            'first:5000 offset:%d){ totalCount nodes { dedupeId } } }'
        ) % (ADVERTISER_ID, offset)
        r = gql(q)
        nodes = r["data"]["allPurchaseAttributions"]["nodes"]
        total = r["data"]["allPurchaseAttributions"]["totalCount"]
        for n in nodes:
            d = n.get("dedupeId")
            if d:
                attr.add(str(d))
        offset += len(nodes)
        if not nodes or len(nodes) < 5000 or offset >= total:
            break
    return attr


def main():
    purchases = fetch_purchases()
    real = [p for p in purchases if is_real_pc_order(p)]
    print(f"Real PC orders: {len(real):,} of {len(purchases):,} total")

    attr_set = fetch_attributed_set()
    print(f"Attributed dedupeIds (all-time): {len(attr_set):,}")

    # =========================================================
    # CITY rollup — keyed by "City, ST" with real lat/lng from pgeocode (ZIP-based).
    # Browser-side captures (scriptId=QwxIISziQhWR) have zip_code in data.
    # Server-side / Stape captures (nubvnAhNMDnm) currently lack a customer
    # zip and geocode to Stape's Iowa AWS server, so they're bucketed as
    # "Unknown / Unmapped" until Meg's team forwards the customer zip.
    # =========================================================
    BROWSER_SCRIPT = "QwxIISziQhWR"
    STAPE_SCRIPT = "nubvnAhNMDnm"

    # ZIP geocode cache so we don't hit pgeocode 2k times
    zip_cache = {}
    def geocode_zip(z):
        if z in zip_cache:
            return zip_cache[z]
        try:
            r = _NOMI.query_postal_code(z)
            lat = r.latitude if r is not None else None
            lng = r.longitude if r is not None else None
            place = r.place_name if r is not None else ""
            state = r.state_code if r is not None else ""
            import math
            if lat is None or (isinstance(lat, float) and math.isnan(lat)):
                lat = None
            if lng is None or (isinstance(lng, float) and math.isnan(lng)):
                lng = None
            res = {"lat": lat, "lng": lng, "city": place or "", "state": state or ""}
        except Exception:
            res = {"lat": None, "lng": None, "city": "", "state": ""}
        zip_cache[z] = res
        return res

    by_city = defaultdict(lambda: {
        "city": "", "state": "", "lat": None, "lng": None,
        "subs": 0, "revenue": 0.0,
        "paidSubs": 0, "organicSubs": 0, "attributedSubs": 0, "redeems": 0,
        "zips": set(),
        "source_browser": 0, "source_stape": 0, "source_other": 0,
    })
    # Stats
    src_counts = Counter()
    stape_unmapped = 0
    zip_missing = 0

    for p in real:
        data = p.get("data") or {}
        sid = p.get("scriptId") or ""
        src_counts[sid] += 1

        zip_code = str(data.get("zip_code") or "").strip()
        m = re.match(r"^(\d{5})", zip_code)
        zip5 = m.group(1) if m else ""

        # Bucket: if no zip OR source is Stape (which delivers no zip and geocodes to Iowa),
        # send to Unknown so the map doesn't get polluted by Council Bluffs.
        if not zip5 or sid == STAPE_SCRIPT:
            key = "Unknown / Unmapped"
            bucket = by_city[key]
            bucket["city"] = "Unknown / Unmapped"
            bucket["state"] = ""
            bucket["lat"] = None
            bucket["lng"] = None
            if sid == STAPE_SCRIPT:
                stape_unmapped += 1
            else:
                zip_missing += 1
        else:
            geo = geocode_zip(zip5)
            city = geo["city"] or (p.get("city") or "Unknown")
            state = geo["state"] or (p.get("region") or "")
            key = f"{city}, {state}" if state else city
            bucket = by_city[key]
            bucket["city"] = city
            bucket["state"] = state
            if bucket["lat"] is None and geo["lat"] is not None:
                bucket["lat"] = geo["lat"]
                bucket["lng"] = geo["lng"]
            bucket["zips"].add(zip5)

        rev = float(p.get("revenue") or 0)
        url = str(p.get("url") or "")
        is_redeem = (rev == 0 or "/redeem_signup/" in url)
        bucket["subs"] += 1
        bucket["revenue"] += rev
        if is_redeem:
            bucket["redeems"] += 1

        utm_src = (data.get("utm_source") or "").lower()
        utm_med = (data.get("utm_medium") or "").lower()
        is_paid = (
            bool(data.get("gclid")) or bool(data.get("fbclid")) or bool(data.get("msclkid"))
            or utm_med in {"cpc", "paid", "display", "social", "video"}
            or utm_src in {"google_sem", "google_ads", "meta", "facebook", "stackadapt"}
        )
        if is_paid: bucket["paidSubs"] += 1
        else:       bucket["organicSubs"] += 1

        if str(p.get("dedupeId") or "") in attr_set:
            bucket["attributedSubs"] += 1

        if sid == BROWSER_SCRIPT:   bucket["source_browser"] += 1
        elif sid == STAPE_SCRIPT:   bucket["source_stape"] += 1
        else:                       bucket["source_other"] += 1

    print(f"\nSource breakdown of real PC orders:")
    for sid, n in src_counts.most_common():
        label = "Browser" if sid == BROWSER_SCRIPT else ("Stape" if sid == STAPE_SCRIPT else sid or "(none)")
        print(f"  {label:<20}  {n:>5}")
    print(f"  Stape captures bucketed as Unknown (no customer zip yet): {stape_unmapped:,}")
    print(f"  Other purchases without zip:                              {zip_missing:,}")

    # Serialize
    city_rollup = {}
    for key, b in by_city.items():
        city_rollup[key] = {
            "name": key,
            "city": b["city"],
            "state": b["state"],
            "lat": b["lat"],
            "lng": b["lng"],
            "subs": b["subs"],
            "revenue": round(b["revenue"], 2),
            "aov": round(b["revenue"] / b["subs"], 2) if b["subs"] else 0,
            "paidSubs": b["paidSubs"],
            "organicSubs": b["organicSubs"],
            "attributedSubs": b["attributedSubs"],
            "redeems": b["redeems"],
            "zips": sorted(b["zips"]),
            "source_browser": b["source_browser"],
            "source_stape": b["source_stape"],
            "source_other": b["source_other"],
        }

    # Sanity counts
    plotted = sum(1 for c in city_rollup.values() if c["lat"] is not None and c["name"] != "Unknown / Unmapped")
    unmapped = city_rollup.get("Unknown / Unmapped", {}).get("subs", 0)
    print(f"\nCity rollup: {len(city_rollup):,} keys, {plotted:,} plottable, {unmapped:,} subs in Unknown bucket")

    # Keep state rollup for the right-rail table on the trade-area page
    by_state = defaultdict(lambda: {"subs": 0, "revenue": 0.0, "paid": 0, "organic": 0})
    for c in city_rollup.values():
        if c["state"]:
            by_state[c["state"]]["subs"] += c["subs"]
            by_state[c["state"]]["revenue"] += c["revenue"]
            by_state[c["state"]]["paid"] += c["paidSubs"]
            by_state[c["state"]]["organic"] += c["organicSubs"]
    state_rollup = {k: {**v, "revenue": round(v["revenue"], 2)} for k, v in by_state.items()}

    top_cities = sorted(
        [c for c in city_rollup.values() if c["name"] != "Unknown / Unmapped"],
        key=lambda x: -x["subs"],
    )[:50]

    # =========================================================
    # Top Recipes — parse /recipe/{slug} from URL, referrer, landing_page
    # of every real PC order. This is the closest thing to a per-product
    # signal we have until PC's tag starts firing per-recipe data.
    # We tally any time a recipe path appears in the customer's journey
    # and dedupe per-purchase so a single order can't double-count.
    # =========================================================
    RECIPE_RE = re.compile(r"/recipe/([a-z0-9][a-z0-9\-_]+)", re.IGNORECASE)
    recipe_counts = Counter()
    recipe_revenue = defaultdict(float)
    for p in real:
        rev = float(p.get("revenue") or 0)
        urls_seen = set()
        for field in ("url", "referrer"):
            v = p.get(field) or ""
            if v: urls_seen.add(str(v))
        data = p.get("data") or {}
        for k in ("landing_page", "referrer"):
            v = data.get(k) or ""
            if v: urls_seen.add(str(v))
        slugs = set()
        for u in urls_seen:
            for m in RECIPE_RE.finditer(u):
                slug = m.group(1).lower().rstrip("?#/")
                # Skip obvious non-recipe slugs that might match the regex
                if slug in {"signup", "share", "search", "list", "all"}: continue
                slugs.add(slug)
        for slug in slugs:
            recipe_counts[slug] += 1
            recipe_revenue[slug] += rev

    def slug_to_title(slug):
        # crispy-lemon-chik-n-with-brown-rice  ->  Crispy Lemon Chik n With Brown Rice
        # Replace separators, title-case, fix common words.
        parts = slug.replace("_", "-").split("-")
        words = [w.capitalize() for w in parts if w]
        return " ".join(words)

    top_recipes = [
        {
            "slug": s,
            "name": slug_to_title(s),
            "subs_touched": recipe_counts[s],
            "revenue_touched": round(recipe_revenue[s], 2),
        }
        for s in sorted(recipe_counts, key=lambda x: -recipe_counts[x])[:30]
    ]

    # =========================================================
    # SKU / Price Tier (kept as a fallback view)
    # =========================================================
    tier_counts = Counter()
    tier_revenue = defaultdict(float)
    item_counts = Counter()
    for p in real:
        rev = float(p.get("revenue") or 0)
        url = str(p.get("url") or "")
        # Exclude redeems (price=0 skews tier)
        if rev == 0 or "/redeem_signup/" in url:
            continue
        tier = tier_for_price(rev)
        tier_counts[tier] += 1
        tier_revenue[tier] += rev
        # Also collect item names if products array is present
        products = p.get("products") or []
        if isinstance(products, list):
            for prod in products:
                name = (prod or {}).get("item_name")
                if name:
                    item_counts[name] += 1

    sku_tiers = [
        {
            "tier": t,
            "subs": tier_counts[t],
            "revenue": round(tier_revenue[t], 2),
            "aov": round(tier_revenue[t] / tier_counts[t], 2) if tier_counts[t] else 0,
        }
        for t in sorted(tier_counts.keys(), key=lambda x: -tier_counts[x])
    ]
    top_items = [{"name": n, "count": c} for n, c in item_counts.most_common(15)]

    # =========================================================
    # Channel overlap: solo vs multi-touch
    # Multi-touch = visit_attribution_available not yet wired, so use simple
    # paid+organic logic from utm flags on the purchase.
    # =========================================================
    overlap = Counter()
    for p in real:
        data = p.get("data") or {}
        signals = set()
        if data.get("gclid") or (data.get("utm_source") or "").lower() in {"google_sem", "google_ads"}:
            signals.add("SEM")
        if data.get("fbclid") or (data.get("utm_source") or "").lower() in {"meta", "facebook"}:
            signals.add("Meta")
        if (data.get("utm_source") or "").lower() == "stackadapt":
            signals.add("Programmatic")
        if (data.get("utm_medium") or "").lower() == "email":
            signals.add("Email")
        if (data.get("utm_medium") or "").lower() == "affiliate":
            signals.add("Affiliate")
        if not signals:
            signals.add("Direct / Organic")
        # Compose key
        key = " + ".join(sorted(signals))
        overlap[key] += 1

    overlap_rows = [
        {"channels": k, "subs": c, "isMulti": "+" in k}
        for k, c in overlap.most_common()
    ]

    # =========================================================
    # Output
    # =========================================================
    out = {
        "version": 3,
        "advertiserId": ADVERTISER_ID,
        "advertiserName": "Purple Carrot",
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "totalSubs":       sum(c["subs"] for c in city_rollup.values()),
            "totalRevenue":    round(sum(c["revenue"] for c in city_rollup.values()), 2),
            "totalAttributed": sum(c["attributedSubs"] for c in city_rollup.values()),
            "uniqueCities":    sum(1 for c in city_rollup.values() if c["name"] != "Unknown / Unmapped"),
            "uniqueStates":    len(state_rollup),
            "unmappedSubs":    city_rollup.get("Unknown / Unmapped", {}).get("subs", 0),
        },
        "cityRollup": city_rollup,
        "stateRollup": state_rollup,
        "topCities":  top_cities,
        "skuTiers":   sku_tiers,
        "topItems":   top_items,
        "topRecipes": top_recipes,
        "channelOverlap": overlap_rows,
    }

    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out, f, indent=2, default=str)
    os.replace(tmp, OUT_PATH)

    print(f"\nWrote: {OUT_PATH}")
    print(f"Summary:")
    print(f"  totalSubs:       {out['summary']['totalSubs']:,}")
    print(f"  totalRevenue:    ${out['summary']['totalRevenue']:,.2f}")
    print(f"  totalAttributed: {out['summary']['totalAttributed']:,}")
    print(f"  uniqueCities:    {out['summary']['uniqueCities']:,}")
    print(f"  uniqueStates:    {out['summary']['uniqueStates']:,}")
    print(f"  unmappedSubs:    {out['summary']['unmappedSubs']:,}")
    print(f"\nSKU tiers:")
    for t in sku_tiers:
        print(f"  {t['tier']:<30} {t['subs']:>5} subs  ${t['revenue']:>10,.2f}  AOV ${t['aov']:.2f}")
    print(f"\nTop channel overlap rows:")
    for row in overlap_rows[:10]:
        print(f"  {row['channels']:<35} {row['subs']:>5}")


if __name__ == "__main__":
    main()
