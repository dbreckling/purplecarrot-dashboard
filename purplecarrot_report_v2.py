"""
Purple Carrot Cross-Channel Campaign Dashboard - Data Report v2
Produces campaign-structured dashboard_data_v2.json for the dynamic dashboard.

Advertiser 1060 — DTC Subscription Meal Kit
Channels: Programmatic (StackAdapt via DLVE) + Purchase Analytics (DLVE Universal Tag)

Data collected via DLVE Universal Tag v14.10.0:
  - ordersuccess (dataLayer primary) — numeric dedupe IDs (e.g. 10051255)
  - cart commit fallback (API intercept) — short alpha IDs (e.g. DBA4zjPy)
  - Identity: email_hash, customer_id, zip_code, logged_in
  - Traffic: event_url (with UTMs), referrer
  - Products: cart_quantity, items array
"""
import time
import re
import json
import os
import requests
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs

# -----------------------------
# Config
# -----------------------------
GRAPHQL_URL = "https://dev.graph.script.flowershop.media/graphql"
USERNAME = "developer"
PASSWORD = "FlowerShop2024DevGraphQL123"

DATA_DIR = os.environ.get("DATA_DIR", ".")
ADVERTISER_ID = os.environ.get("ADVERTISER_ID", "1060")

LOCAL_TZ = ZoneInfo("America/New_York")  # Purple Carrot is East Coast

START_DATE_STR = os.environ.get("START_DATE", "2026-03-01")
END_DATE_STR = os.environ.get("END_DATE", "")

REQUEST_TIMEOUT_SECONDS = 120
MAX_RETRIES = 3

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")
UPLOAD_SECRET = os.environ.get("UPLOAD_SECRET", "dev-secret-key")

# Programmatic CPM by DLVE placement (StackAdapt)
PLACEMENT_CONFIG = {
    "6001": {"channel": "ctv", "cpm": 28.00},
    "6010": {"channel": "display", "cpm": 6.00},
    "6011": {"channel": "video", "cpm": 7.50},
}
DEFAULT_CPM = 6.00  # fallback for unknown placements

# SA pixel keys (for reference in reporting)
SAQ_CONV_KEY = "xt5chVHtrnw0wJZcvE7Ael"
SAQ_RT_SID = "5dEaYPe2HyCbyWjEIxHtem"


# -----------------------------
# Helpers
# -----------------------------
def is_real_order_id(x):
    if x is None:
        return False
    x = str(x).strip()
    if not x:
        return False
    # Real Purple Carrot transactionId: 7-digit numeric starting with 297/298/299
    if x.isdigit() and len(x) == 7 and x[:3] in ("297", "298", "299"):
        return True
    # Also accept other numeric 7-8 digit IDs (cart.id from fetch intercept)
    if x.isdigit() and len(x) >= 7:
        return True
    # Cart fallback IDs: short alphanumeric (e.g. DBA4zjPy, GMzOxoEn)
    if len(x) >= 6 and x.isalnum():
        return True
    return False


def is_real_transaction_id(x):
    """Check if a dedupeId is a real Purple Carrot transactionId (from ordersuccess dataLayer event).
    Real transactionIds are 7-digit numeric starting with 297/298/299."""
    if not x:
        return False
    d = str(x).strip()
    return d.isdigit() and len(d) == 7 and d[:3] in ("297", "298", "299")


def classify_order_source(dedupe_id):
    """Classify whether order came via ordersuccess or cart fallback."""
    if not dedupe_id:
        return "unknown"
    d = str(dedupe_id).strip()
    # Real transactionId from ordersuccess dataLayer event (297/298/299 prefix)
    if is_real_transaction_id(d):
        return "ordersuccess"
    # Cart.id from fetch intercept (100xxxxx format — NOT real transactions)
    if d.isdigit() and len(d) >= 7:
        return "cart_save"
    # Alpha IDs from cart fallback (e.g. DBA4zjPy)
    if len(d) <= 10 and d.isalnum() and not d.isdigit():
        return "cart_fallback"
    return "unknown"


def extract_utm_params(url):
    """Extract UTM parameters from a URL."""
    if not url:
        return {}
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        utms = {}
        for key in ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"):
            vals = params.get(key, [])
            if vals:
                utms[key] = vals[0]
        # Also capture gclid for Google Ads attribution
        if params.get("gclid"):
            utms["gclid"] = params["gclid"][0]
        return utms
    except Exception:
        return {}


def classify_traffic_source(url, referrer):
    """Classify traffic source from URL params and referrer."""
    utms = extract_utm_params(url)
    source = utms.get("utm_source", "").lower()
    medium = utms.get("utm_medium", "").lower()

    if utms.get("gclid") or source == "google" and medium == "paid":
        return "Google Ads"
    if source == "google" and medium in ("cpc", "paid"):
        return "Google Ads"
    if source in ("facebook", "fb", "instagram", "ig") and medium in ("paid", "cpc", "social"):
        return "Meta Ads"
    if source == "stackadapt" or medium == "programmatic":
        return "StackAdapt"
    if medium in ("paid", "cpc", "cpm", "display"):
        return f"Paid ({source or 'unknown'})"
    if medium == "email":
        return "Email"
    if medium in ("organic", "social"):
        return f"Organic ({source or 'social'})"

    # Fallback to referrer analysis
    ref = (referrer or "").lower()
    if "google.com" in ref:
        return "Google Organic"
    if "facebook.com" in ref or "instagram.com" in ref:
        return "Meta Organic"
    if "bing.com" in ref:
        return "Bing Organic"
    if "purplecarrot.com" in ref:
        return "Direct / Internal"
    if ref and ref != "none":
        return "Referral"
    return "Direct"


def parse_data_blob(node):
    """Parse the JSON data blob from a purchase node."""
    raw = node.get("data")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def normalize_visitor_status(value):
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    if v in ("new", "new visitor", "new_customer", "first"):
        return "new"
    if v in ("returning", "returning visitor", "existing", "repeat"):
        return "returning"
    if "new" in v:
        return "new"
    if "return" in v or "existing" in v:
        return "returning"
    return None


def extract_logged_in_status(data_blob):
    """Extract logged_in status from data blob. Returns 'yes', 'no', or 'unknown'."""
    li = data_blob.get("logged_in", "")
    if not li:
        return "unknown"
    li = str(li).strip()
    if li.upper().startswith("YES"):
        return "yes"
    if li.lower() == "no":
        return "no"
    return "unknown"


# -----------------------------
# GraphQL Queries
# -----------------------------
def run_query_graphql(query, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                json={"query": query},
                auth=(USERNAME, PASSWORD),
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            if resp.status_code != 200:
                print(f"HTTP Error {resp.status_code}: {resp.text[:400]}")
                return None
            data = resp.json()
            if "errors" in data:
                print("Query error:", data["errors"])
                return None
            return data.get("data", {})
        except requests.exceptions.ReadTimeout:
            wait = 3 * attempt
            print(f"[Timeout] attempt {attempt}/{max_retries} ... retrying in {wait}s")
            time.sleep(wait)
        except Exception as e:
            print(f"[Error] {type(e).__name__}: {e}")
            return None
    print("[Failed] Max retries reached.")
    return None


def run_purchases_query(date_from, date_to, max_retries=MAX_RETRIES):
    all_nodes = []
    offset = 0
    page_size = 5000

    while True:
        query = f"""
        query {{
          allPurchases(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              time: {{
                greaterThanOrEqualTo: "{date_from}"
                lessThan: "{date_to}"
              }}
            }}
            first: {page_size}
            offset: {offset}
            orderBy: TIME_DESC
          ) {{
            nodes {{
              time
              url
              revenue
              data
              osName
              countryCode
              region
              city
              dedupeId
              visitorId
              visitorStatus
              cartQty
              scriptId
              scriptType
              scriptVersion
              referrer
              products
              ip
              currency
            }}
            totalCount
          }}
        }}
        """

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(
                    GRAPHQL_URL,
                    json={"query": query},
                    auth=(USERNAME, PASSWORD),
                    timeout=REQUEST_TIMEOUT_SECONDS
                )
                if resp.status_code != 200:
                    return all_nodes
                data = resp.json()
                if "errors" in data:
                    return all_nodes
                nodes = data.get("data", {}).get("allPurchases", {}).get("nodes", []) or []
                total_count = data.get("data", {}).get("allPurchases", {}).get("totalCount", 0)
                all_nodes.extend(nodes)
                if len(all_nodes) >= total_count or len(nodes) < page_size:
                    return all_nodes
                offset += page_size
                break
            except requests.exceptions.ReadTimeout:
                wait = 3 * attempt
                print(f"[Timeout] attempt {attempt}/{max_retries} ... retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                print(f"[Error] {type(e).__name__}: {e}")
                return all_nodes
        else:
            return all_nodes

    return all_nodes


def run_impressions_query(date_from, date_to, max_retries=MAX_RETRIES):
    """Query all impressions for advertiser 1060 (StackAdapt programmatic)."""
    limit = 10000
    offset = 0
    all_nodes = []
    total_count = 0

    while True:
        query = f"""
        query {{
          allImpressions(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              time: {{
                greaterThanOrEqualTo: "{date_from}"
                lessThan: "{date_to}"
              }}
            }}
            first: {limit}
            offset: {offset}
          ) {{
            nodes {{
              time
              ip
              ipUserType
              countryCode
              region
              campaignId
              campaignName
              placementId
              data
            }}
            totalCount
          }}
        }}
        """

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(
                    GRAPHQL_URL,
                    json={"query": query},
                    auth=(USERNAME, PASSWORD),
                    timeout=REQUEST_TIMEOUT_SECONDS
                )
                if resp.status_code != 200:
                    return all_nodes, total_count
                data = resp.json()
                if "errors" in data:
                    return all_nodes, total_count
                nodes = data.get("data", {}).get("allImpressions", {}).get("nodes", []) or []
                total_count = data.get("data", {}).get("allImpressions", {}).get("totalCount", total_count)
                all_nodes.extend(nodes)
                if len(nodes) < limit:
                    return all_nodes, max(total_count, len(all_nodes))
                offset += limit
                break
            except requests.exceptions.ReadTimeout:
                wait = 3 * attempt
                print(f"[Timeout] attempt {attempt}/{max_retries} ... retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                print(f"[Error] {type(e).__name__}: {e}")
                return all_nodes, total_count
        else:
            return all_nodes, total_count


def run_clicks_query(date_from, date_to, max_retries=MAX_RETRIES):
    """Query all clicks for advertiser 1060."""
    limit = 10000
    offset = 0
    all_nodes = []
    total_count = 0

    while True:
        query = f"""
        query {{
          allClicks(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              time: {{
                greaterThanOrEqualTo: "{date_from}"
                lessThan: "{date_to}"
              }}
            }}
            first: {limit}
            offset: {offset}
          ) {{
            nodes {{
              time
              ip
              ipUserType
              countryCode
              region
              referrer
              campaignId
              campaignName
              placementId
              data
            }}
            totalCount
          }}
        }}
        """

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(
                    GRAPHQL_URL,
                    json={"query": query},
                    auth=(USERNAME, PASSWORD),
                    timeout=REQUEST_TIMEOUT_SECONDS
                )
                if resp.status_code != 200:
                    return all_nodes, total_count
                data = resp.json()
                if "errors" in data:
                    return all_nodes, total_count
                nodes = data.get("data", {}).get("allClicks", {}).get("nodes", []) or []
                total_count = data.get("data", {}).get("allClicks", {}).get("totalCount", total_count)
                all_nodes.extend(nodes)
                if len(nodes) < limit:
                    return all_nodes, max(total_count, len(all_nodes))
                offset += limit
                break
            except requests.exceptions.ReadTimeout:
                wait = 3 * attempt
                print(f"[Timeout] attempt {attempt}/{max_retries} ... retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                print(f"[Error] {type(e).__name__}: {e}")
                return all_nodes, total_count
        else:
            return all_nodes, total_count


def run_attributed_query(date_from, date_to, max_retries=MAX_RETRIES):
    """Fetch attributed conversions using allPurchaseAttributions
    enriched with metadata from allLastTouchMatches."""
    all_nodes = []
    offset = 0
    page_size = 5000

    while True:
        query = f"""
        query {{
          allPurchaseAttributions(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              time: {{
                greaterThanOrEqualTo: "{date_from}"
                lessThan: "{date_to}"
              }}
            }}
            first: {page_size}
            offset: {offset}
          ) {{
            nodes {{
              revenue
              time
              dedupeId
            }}
            totalCount
          }}
        }}
        """
        data = run_query_graphql(query)
        if not data or not data.get("allPurchaseAttributions"):
            break

        nodes = data["allPurchaseAttributions"]["nodes"]
        all_nodes.extend(nodes)
        total_count = data["allPurchaseAttributions"].get("totalCount", 0)

        if len(nodes) < page_size or len(all_nodes) >= total_count:
            break
        offset += page_size

    # Enrich with lastTouchMatches
    if all_nodes:
        ltm_query = f"""
        query {{
          allLastTouchMatches(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              time: {{
                greaterThanOrEqualTo: "{date_from}"
                lessThan: "{date_to}"
              }}
            }}
            first: 5000
          ) {{
            nodes {{
              dedupeId
              cartQty
              impressionCampaignId
              impressionCampaignName
              impressionPlacementId
              impressionTime
              visitorStatus
              visitorId
            }}
          }}
        }}
        """
        ltm_data = run_query_graphql(ltm_query)
        ltm_lookup = {}
        if ltm_data and ltm_data.get("allLastTouchMatches"):
            for n in ltm_data["allLastTouchMatches"]["nodes"]:
                did = n.get("dedupeId")
                if did:
                    ltm_lookup[did] = n

        enriched = 0
        for node in all_nodes:
            did = node.get("dedupeId")
            if did and did in ltm_lookup:
                ltm = ltm_lookup[did]
                for field in ("cartQty", "impressionCampaignId", "impressionCampaignName",
                              "impressionPlacementId", "impressionTime",
                              "visitorStatus", "visitorId"):
                    if not node.get(field):
                        node[field] = ltm.get(field, "")
                enriched += 1

    return all_nodes


def fetch_visitor_histories(visitor_ids, start_date_str, end_date_str):
    """Query allPurchases per visitor to determine new vs returning."""
    results = {}
    total = len(visitor_ids)
    if total == 0:
        return results

    print(f"\n  Fetching visitor purchase history for {total} visitor(s)...")
    for i, vid in enumerate(visitor_ids, 1):
        query = f"""
        query {{
          allPurchases(
            filter: {{
              advertiserId: {{ equalTo: "{ADVERTISER_ID}" }}
              visitorId: {{ equalTo: "{vid}" }}
              time: {{
                greaterThanOrEqualTo: "{start_date_str}"
                lessThan: "{end_date_str}"
              }}
            }}
            orderBy: TIME_ASC
          ) {{
            totalCount
            nodes {{
              time
              revenue
              isReturning
            }}
          }}
        }}
        """
        data = run_query_graphql(query)
        if data:
            purchases = data.get("allPurchases", {})
            nodes = purchases.get("nodes", [])
            total_count = purchases.get("totalCount", 0)
            is_returning = any(n.get("isReturning") for n in nodes) if nodes else False
            results[vid] = {
                "purchaseCount": total_count,
                "isReturning": is_returning,
                "firstPurchaseAt": nodes[0]["time"] if nodes else None,
                "lastPurchaseAt": nodes[-1]["time"] if nodes else None,
            }
        else:
            results[vid] = {"purchaseCount": 0, "isReturning": False, "firstPurchaseAt": None, "lastPurchaseAt": None}

    returning_count = sum(1 for v in results.values() if v["isReturning"])
    new_count = sum(1 for v in results.values() if not v["isReturning"])
    print(f"  Visitor history: {new_count} new, {returning_count} returning (of {total} visitors)")
    return results


# -----------------------------
# Click / impression filtering
# -----------------------------
def filter_clean_clicks(click_nodes, geo_filter="US"):
    filtered = []
    for n in click_nodes:
        ip_type = n.get("ipUserType", "")
        country = n.get("countryCode", "")
        if ip_type in ("hosting", "business"):
            continue
        if geo_filter and country != geo_filter:
            continue
        filtered.append(n)

    ip_counts = Counter()
    clean = []
    for n in filtered:
        ip = n.get("ip", "")
        ip_counts[ip] += 1
        if ip_counts[ip] <= 3:
            clean.append(n)

    return clean, len(click_nodes) - len(clean)


def filter_clean_impressions(imp_nodes, geo_filter=None):
    clean = []
    for n in imp_nodes:
        country = n.get("countryCode", "")
        if geo_filter and country and country != geo_filter:
            continue
        clean.append(n)
    return clean, len(imp_nodes) - len(clean)


# -----------------------------
# Main
# -----------------------------
def main():
    print(f"Purple Carrot Cross-Channel Dashboard v2 (Advertiser {ADVERTISER_ID})")
    print("=" * 70)
    print("DTC Subscription Model — Purchase Analytics + Programmatic Attribution\n")

    # Determine date range
    if START_DATE_STR:
        start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)
    else:
        start_date = (datetime.now(LOCAL_TZ) - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)

    run_start_date = start_date.date()
    run_end_date = datetime.now(LOCAL_TZ).date()

    print(f"Date Range: {run_start_date} to {run_end_date} (Eastern Time)\n")

    end_date_exclusive = datetime.combine(run_end_date, datetime.min.time(), tzinfo=LOCAL_TZ) + timedelta(days=1)

    # -------------------------
    # Fetch data day by day
    # -------------------------
    current = datetime.combine(run_start_date, datetime.min.time(), tzinfo=LOCAL_TZ)

    all_impressions = []
    all_clicks = []
    all_purchases = []
    all_attributed = []
    total_impressions_count = 0
    total_clicks_count = 0

    while current < end_date_exclusive:
        day_start_utc = current.astimezone(ZoneInfo("UTC"))
        day_end_utc = (current + timedelta(days=1)).astimezone(ZoneInfo("UTC"))
        day_start = day_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        day_end = day_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        day_label = current.strftime("%Y-%m-%d")

        # Purchases
        purchase_nodes = run_purchases_query(day_start, day_end)
        all_purchases.extend(purchase_nodes)

        # Impressions (StackAdapt programmatic)
        imp_nodes, imp_total = run_impressions_query(day_start, day_end)
        all_impressions.extend([(n.get("campaignId", ""), n.get("placementId", ""), n) for n in imp_nodes])
        total_impressions_count += len(imp_nodes)

        # Clicks
        click_nodes, click_total = run_clicks_query(day_start, day_end)
        all_clicks.extend([(n.get("campaignId", ""), n.get("placementId", ""), n) for n in click_nodes])
        total_clicks_count += len(click_nodes)

        # Attributed conversions
        attr_nodes = run_attributed_query(day_start, day_end)
        all_attributed.extend(attr_nodes)

        print(f"{day_label} ... {len(imp_nodes):,} imp, {len(click_nodes):,} clicks, {len(purchase_nodes)} purchases, {len(attr_nodes)} attributed")
        current += timedelta(days=1)

    # -------------------------
    # Dedupe purchases:
    #   Only count real ordersuccess transactions (297/298/299 prefix transactionIds)
    #   These are confirmed new subscription events from the dataLayer.
    #   Cart.id (100xxxxx) and cart fallback (alpha) are cart save/modify activity.
    # -------------------------
    seen_ids = set()
    unique_purchases = []
    skipped_cart_saves = 0
    skipped_cart_fallback_purchases = 0
    for p in all_purchases:
        did = p.get("dedupeId")
        if not did:
            continue
        if did in seen_ids:
            continue
        seen_ids.add(did)
        source = classify_order_source(did)
        if source == "ordersuccess":
            unique_purchases.append(p)
        elif source == "cart_save":
            skipped_cart_saves += 1
        elif source == "cart_fallback":
            skipped_cart_fallback_purchases += 1

    print(f"\n  Purchases: {len(seen_ids)} unique dedupeIds")
    print(f"  Real ordersuccess (297/298/299): {len(unique_purchases)}")
    print(f"  Skipped cart saves (100xxxxx): {skipped_cart_saves}")
    print(f"  Skipped cart fallback (alpha): {skipped_cart_fallback_purchases}")

    # Stage 1: Dedupe by dedupeId, ordersuccess only (7+ digit numeric DLV-transactionId)
    # Cart fallback IDs are cart add/remove activity, not completed transactions
    seen_attr_ids = set()
    ordersuccess_attributed = []
    skipped_cart_fallback = 0
    for a in all_attributed:
        did = a.get("dedupeId")
        if did and did not in seen_attr_ids:
            seen_attr_ids.add(did)
            if classify_order_source(did) == "ordersuccess":
                ordersuccess_attributed.append(a)
            else:
                skipped_cart_fallback += 1

    # Stage 2: Visitor-window dedup (30-min window, keep last event per window)
    # Same visitor firing multiple ordersuccess events within minutes = same order
    # with minor tax/fee recalculations. Keep the last event (final transaction amount).
    DEDUP_WINDOW_SECONDS = 1800  # 30 minutes
    visitor_groups = defaultdict(list)
    for a in ordersuccess_attributed:
        vid = a.get("visitorId") or a.get("ip", "unknown")
        visitor_groups[vid].append(a)

    unique_attributed = []
    visitor_dedup_removed = 0
    for vid, events in visitor_groups.items():
        events.sort(key=lambda x: x.get("time", ""))
        current_window = [events[0]]
        for e in events[1:]:
            try:
                prev_time = datetime.fromisoformat(current_window[-1].get("time", "").replace("Z", "+00:00"))
                curr_time = datetime.fromisoformat(e.get("time", "").replace("Z", "+00:00"))
                gap = (curr_time - prev_time).total_seconds()
            except Exception:
                gap = 99999
            if gap <= DEDUP_WINDOW_SECONDS:
                current_window.append(e)
            else:
                unique_attributed.append(current_window[-1])
                visitor_dedup_removed += len(current_window) - 1
                current_window = [e]
        unique_attributed.append(current_window[-1])
        visitor_dedup_removed += len(current_window) - 1

    print(f"\n  Total unique purchases: {len(unique_purchases)}")
    print(f"  Attributed: {len(ordersuccess_attributed)} ordersuccess, {skipped_cart_fallback} cart fallback skipped")
    print(f"  Visitor-window dedup (30min): removed {visitor_dedup_removed} re-fires, final: {len(unique_attributed)}")

    # -------------------------
    # Enrich purchases with data blob
    # -------------------------
    for p in unique_purchases:
        blob = parse_data_blob(p)
        p["_data"] = blob
        p["_traffic_source"] = classify_traffic_source(p.get("url", ""), p.get("referrer", ""))
        p["_utms"] = extract_utm_params(p.get("url", ""))
        p["_order_source"] = classify_order_source(p.get("dedupeId", ""))
        p["_logged_in"] = extract_logged_in_status(blob)
        p["_email_hash"] = blob.get("email_hash", "")
        p["_customer_id"] = blob.get("customer_id", "")
        p["_zip_code"] = blob.get("zip_code", p.get("zipCode", ""))

    # -------------------------
    # Process impressions and clicks (US-only filtering)
    # -------------------------
    raw_imp_nodes = [n for _, _, n in all_impressions]
    raw_click_nodes = [n for _, _, n in all_clicks]
    clean_impressions_list, filtered_imp = filter_clean_impressions(raw_imp_nodes)
    clean_clicks_list, filtered_click = filter_clean_clicks(raw_click_nodes)

    imp_count = len(clean_impressions_list)
    click_count = len(clean_clicks_list)

    # Build clean click set (by id(node)) for fast lookup, and
    # rebuild all_clicks_clean with campaign association for downstream use
    clean_click_ids = set(id(n) for n in clean_clicks_list)
    all_clicks_clean = [(cid, pid, n) for cid, pid, n in all_clicks if id(n) in clean_click_ids]

    print(f"  Click filtering: {len(raw_click_nodes)} raw → {click_count} clean (US-only, no bots, IP deduped)")

    # Discover campaigns from impressions
    campaign_info = {}
    for _, _, n in all_impressions:
        cid = n.get("campaignId", "")
        if cid and cid not in campaign_info:
            cname = n.get("campaignName", "")
            campaign_info[cid] = {
                "name": cname or f"Campaign {cid}",
                "label": cname or f"Campaign {cid}",
            }
    for _, _, n in all_clicks_clean:
        cid = n.get("campaignId", "")
        if cid and cid not in campaign_info:
            cname = n.get("campaignName", "")
            campaign_info[cid] = {
                "name": cname or f"Campaign {cid}",
                "label": cname or f"Campaign {cid}",
            }

    has_programmatic = imp_count > 0 or click_count > 0
    print(f"\n  Programmatic data: {'YES' if has_programmatic else 'NO'} ({imp_count:,} clean imp, {click_count:,} clean clicks)")
    if campaign_info:
        print(f"  Discovered {len(campaign_info)} campaign(s):")
        for cid, info in sorted(campaign_info.items()):
            print(f"    Campaign {cid}: {info['label']}")

    # -------------------------
    # Purchase Analytics
    # -------------------------
    total_revenue = sum(float(p.get("revenue") or 0) for p in unique_purchases)
    total_orders = len(unique_purchases)
    aov = round(total_revenue / total_orders, 2) if total_orders > 0 else 0

    # Order source breakdown
    source_counts = Counter(p["_order_source"] for p in unique_purchases)

    # Traffic source breakdown
    traffic_counts = Counter(p["_traffic_source"] for p in unique_purchases)
    traffic_revenue = defaultdict(float)
    for p in unique_purchases:
        traffic_revenue[p["_traffic_source"]] += float(p.get("revenue") or 0)

    # Logged in breakdown
    logged_in_counts = Counter(p["_logged_in"] for p in unique_purchases)

    # Identity coverage
    has_email = sum(1 for p in unique_purchases if p["_email_hash"])
    has_customer_id = sum(1 for p in unique_purchases if p["_customer_id"])
    has_zip = sum(1 for p in unique_purchases if p["_zip_code"])

    email_coverage = round(has_email / total_orders * 100, 1) if total_orders > 0 else 0
    customer_id_coverage = round(has_customer_id / total_orders * 100, 1) if total_orders > 0 else 0
    zip_coverage = round(has_zip / total_orders * 100, 1) if total_orders > 0 else 0

    # Cart analysis
    cart_sizes = [int(p.get("cartQty") or 0) for p in unique_purchases if p.get("cartQty")]
    avg_cart_size = round(sum(cart_sizes) / len(cart_sizes), 1) if cart_sizes else 0

    # New vs returning (by visitor — first order = new, subsequent = returning)
    seen_visitors = set()
    new_orders = 0
    returning_orders = 0
    for p in sorted(unique_purchases, key=lambda x: x.get("time", "")):
        vid = p.get("visitorId", "")
        if vid and vid in seen_visitors:
            returning_orders += 1
        else:
            new_orders += 1
            if vid:
                seen_visitors.add(vid)
    new_pct = round(new_orders / total_orders * 100, 1) if total_orders > 0 else 0

    # Geographic breakdown (from city/region)
    geo_region = Counter()
    geo_city = Counter()
    geo_revenue = defaultdict(float)
    for p in unique_purchases:
        region = p.get("region") or "Unknown"
        city = p.get("city") or "Unknown"
        rev = float(p.get("revenue") or 0)
        geo_region[region] += 1
        geo_city[city] += 1
        geo_revenue[region] += rev

    # Device breakdown
    device_counts = Counter(p.get("osName") or "Unknown" for p in unique_purchases)

    # -------------------------
    # Visitor-level analysis
    # -------------------------
    visitor_purchase_map = defaultdict(list)
    for p in unique_purchases:
        vid = p.get("visitorId", "")
        if vid:
            visitor_purchase_map[vid].append(p)

    total_unique_visitors = len(visitor_purchase_map)
    repeat_visitors = sum(1 for v in visitor_purchase_map.values() if len(v) > 1)
    repeat_rate = round(repeat_visitors / total_unique_visitors * 100, 1) if total_unique_visitors > 0 else 0

    # -------------------------
    # Attribution analysis (if programmatic data exists)
    # -------------------------
    attr_count = len(unique_attributed)
    attr_revenue = sum(float(a.get("revenue") or 0) for a in unique_attributed)

    # Build click visitor set for CT vs VT classification (clean US clicks only)
    click_visitor_set = set()
    for _, _, n in all_clicks_clean:
        vid = n.get("visitorId") or n.get("ip", "")
        if vid:
            click_visitor_set.add(vid)

    attr_click_through = 0
    attr_view_through = 0
    for a in unique_attributed:
        vid = a.get("visitorId") or ""
        if vid in click_visitor_set:
            attr_click_through += 1
        else:
            attr_view_through += 1

    # Reach & frequency
    unique_ips = set(n.get("ip") for n in clean_impressions_list if n.get("ip"))
    prog_reach = len(unique_ips)
    prog_frequency = round(imp_count / prog_reach, 2) if prog_reach > 0 else 0

    # Spend estimate (per-placement CPM)
    clean_imp_ids = set(id(n) for n in clean_impressions_list)
    prog_spend = 0
    for _, pid, n in all_impressions:
        if id(n) in clean_imp_ids:
            cpm = PLACEMENT_CONFIG.get(pid, {}).get("cpm", DEFAULT_CPM)
            prog_spend += cpm / 1000
    prog_spend = round(prog_spend, 2)
    prog_roas = round(attr_revenue / prog_spend, 2) if prog_spend > 0 else 0
    prog_cpa = round(prog_spend / attr_count, 2) if attr_count > 0 else 0

    # Days-to-conversion
    days_to_conversion = []
    same_day_count = 0
    for a in unique_attributed:
        imp_time_str = a.get("impressionTime")
        conv_time_str = a.get("time")
        if imp_time_str and conv_time_str:
            try:
                imp_time = pd.to_datetime(imp_time_str, utc=True)
                conv_time = pd.to_datetime(conv_time_str, utc=True)
                delta_hours = (conv_time - imp_time).total_seconds() / 3600
                delta_days = delta_hours / 24
                days_to_conversion.append(delta_days)
                if delta_hours < 24:
                    same_day_count += 1
            except Exception:
                pass

    avg_days_to_conv = round(sum(days_to_conversion) / len(days_to_conversion), 1) if days_to_conversion else 0

    # -------------------------
    # Impressions-to-conversion (how many ads did each converter see before purchasing?)
    # -------------------------
    # Build IP index of impressions (IP → list of impression times)
    imp_by_ip = defaultdict(list)
    for imp_node in clean_impressions_list:
        ip = imp_node.get("ip")
        imp_time_raw = imp_node.get("time")
        if ip and imp_time_raw:
            try:
                imp_by_ip[ip].append(pd.to_datetime(imp_time_raw, utc=True))
            except Exception:
                imp_by_ip[ip].append(None)

    # Build dedupeId → purchase lookup (to get IP for attributed orders)
    purchase_by_dedupe = {}
    for p in unique_purchases:
        did = p.get("dedupeId")
        if did:
            purchase_by_dedupe[did] = p

    # -------------------------
    # Count view-through site visitors from allVwVisits (DLVE native VT table)
    # -------------------------
    # allVwVisits contains visits from users who were previously served an impression
    # and later visited the site. Filtered to US only, deduped by visitorId.
    ct_unique_visitors = 0
    vt_unique_visitors = 0

    try:
        vt_visitor_ids = set()
        day_cursor = datetime.combine(run_start_date, datetime.min.time(), tzinfo=LOCAL_TZ)
        while day_cursor < end_date_exclusive:
            day_start_utc = day_cursor.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
            day_end_utc = (day_cursor + timedelta(days=1)).astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

            offset = 0
            while True:
                vw_query = """query {
                  allVwVisits(
                    filter: { advertiserId: { equalTo: "%s" }
                      eventTime: { greaterThanOrEqualTo: "%s" lessThan: "%s" }
                      countryCode: { equalTo: "US" } }
                    first: 5000 offset: %d
                  ) { totalCount nodes { visitorId } }
                }""" % (ADVERTISER_ID, day_start_utc, day_end_utc, offset)

                vw_result = run_query_graphql(vw_query)
                if not vw_result:
                    break
                vw_data = vw_result.get("allVwVisits", {})
                vw_nodes = vw_data.get("nodes", [])
                vw_total = vw_data.get("totalCount", 0)
                for n in vw_nodes:
                    vid = n.get("visitorId", "")
                    if vid:
                        vt_visitor_ids.add(vid)
                offset += len(vw_nodes)
                if offset >= vw_total or not vw_nodes:
                    break
            day_cursor += timedelta(days=1)

        vt_unique_visitors = len(vt_visitor_ids)
        # Click-through = clean ad clicks (already tracked separately)
        click_visitor_ids = set()
        for _, _, n in all_clicks_clean:
            vid = n.get("visitorId") or n.get("ip", "")
            if vid:
                click_visitor_ids.add(vid)
        ct_unique_visitors = len(click_visitor_ids)
        print(f"  Click-through visitors: {ct_unique_visitors}, View-through visitors: {vt_unique_visitors}")

    except Exception as e:
        print(f"  Warning: Failed to compute ad-exposed visits: {e}")

    print(f"  Ad-exposed visits: {ct_unique_visitors} click-through, {vt_unique_visitors} view-through")

    # For each attributed order, count impressions seen by that IP before conversion
    converter_impression_counts = []
    converter_details = []
    for a in unique_attributed:
        conv_time_str = a.get("time")
        did = a.get("dedupeId", "")
        vid = a.get("visitorId", "")

        # Get IP from the purchase record
        purchase_rec = purchase_by_dedupe.get(did, {})
        converter_ip = purchase_rec.get("ip") or ""

        if not converter_ip or not conv_time_str:
            continue

        try:
            conv_time = pd.to_datetime(conv_time_str, utc=True)
        except Exception:
            continue

        # Count impressions from this IP that occurred before conversion
        ip_impressions = imp_by_ip.get(converter_ip, [])
        pre_conv_count = sum(1 for t in ip_impressions if t is not None and t <= conv_time)

        converter_impression_counts.append(pre_conv_count)
        converter_details.append({
            "dedupeId": did,
            "visitorId": vid,
            "ip": converter_ip[:8] + "***",  # partially mask IP
            "conversionTime": conv_time_str,
            "impressionsSeen": pre_conv_count,
            "revenue": float(a.get("revenue") or 0),
            "type": "click-through" if vid in click_visitor_set else "view-through",
            "orderSource": classify_order_source(did),
        })

    avg_imps_to_convert = round(sum(converter_impression_counts) / len(converter_impression_counts), 1) if converter_impression_counts else 0
    min_imps_to_convert = min(converter_impression_counts) if converter_impression_counts else 0
    max_imps_to_convert = max(converter_impression_counts) if converter_impression_counts else 0
    median_imps_to_convert = round(sorted(converter_impression_counts)[len(converter_impression_counts) // 2], 1) if converter_impression_counts else 0

    # Frequency buckets for converters
    freq_buckets = {"1-3": 0, "4-7": 0, "8-14": 0, "15-30": 0, "31+": 0}
    for c in converter_impression_counts:
        if c <= 3:
            freq_buckets["1-3"] += 1
        elif c <= 7:
            freq_buckets["4-7"] += 1
        elif c <= 14:
            freq_buckets["8-14"] += 1
        elif c <= 30:
            freq_buckets["15-30"] += 1
        else:
            freq_buckets["31+"] += 1

    print(f"\n  Impressions-to-conversion analysis:")
    print(f"    Converters matched: {len(converter_impression_counts)}")
    print(f"    Avg impressions before purchase: {avg_imps_to_convert}")
    print(f"    Min: {min_imps_to_convert}, Max: {max_imps_to_convert}, Median: {median_imps_to_convert}")
    for detail in converter_details:
        print(f"    • Order {detail['dedupeId']}: {detail['impressionsSeen']} impressions seen, ${detail['revenue']:.2f} rev ({detail['type']})")

    # Conversion timeline buckets
    timeline = {"sameDay": 0, "1day": 0, "2to3days": 0, "4to7days": 0, "8to14days": 0, "15to30days": 0}
    for d in days_to_conversion:
        if d < 1:
            timeline["sameDay"] += 1
        elif d < 2:
            timeline["1day"] += 1
        elif d < 4:
            timeline["2to3days"] += 1
        elif d < 8:
            timeline["4to7days"] += 1
        elif d < 15:
            timeline["8to14days"] += 1
        else:
            timeline["15to30days"] += 1

    # -------------------------
    # Daily data
    # -------------------------
    daily_data = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "purchases": 0, "revenue": 0.0,
        "conversions": 0, "attrRevenue": 0.0,
        "newOrders": 0, "returningOrders": 0,
        "nyPurchases": 0,
        "trafficSources": defaultdict(int),
    })

    for p in unique_purchases:
        t = p.get("time", "")
        if not t:
            continue
        try:
            dt = pd.to_datetime(t, utc=True).astimezone(LOCAL_TZ)
            day_key = dt.strftime("%Y-%m-%d")
        except Exception:
            continue
        rev = float(p.get("revenue") or 0)
        daily_data[day_key]["purchases"] += 1
        daily_data[day_key]["revenue"] += rev
        daily_data[day_key]["trafficSources"][p["_traffic_source"]] += 1
        if p.get("region") == "NY":
            daily_data[day_key]["nyPurchases"] += 1

        vs = normalize_visitor_status(p.get("visitorStatus"))
        if vs == "new":
            daily_data[day_key]["newOrders"] += 1
        elif vs == "returning":
            daily_data[day_key]["returningOrders"] += 1

    for _, _, imp in all_impressions:
        t = imp.get("time", "")
        if not t:
            continue
        try:
            dt = pd.to_datetime(t, utc=True).astimezone(LOCAL_TZ)
            day_key = dt.strftime("%Y-%m-%d")
        except Exception:
            continue
        daily_data[day_key]["impressions"] += 1

    for _, _, click in all_clicks_clean:
        t = click.get("time", "")
        if not t:
            continue
        try:
            dt = pd.to_datetime(t, utc=True).astimezone(LOCAL_TZ)
            day_key = dt.strftime("%Y-%m-%d")
        except Exception:
            continue
        daily_data[day_key]["clicks"] += 1

    for a in unique_attributed:
        t = a.get("time", "")
        if not t:
            continue
        try:
            dt = pd.to_datetime(t, utc=True)
            day_key = dt.strftime("%Y-%m-%d")
        except Exception:
            continue
        rev = float(a.get("revenue") or 0)
        daily_data[day_key]["conversions"] += 1
        daily_data[day_key]["attrRevenue"] += rev

    # Serialize daily data
    serialized_daily = {}
    for day_key, day in sorted(daily_data.items()):
        serialized_daily[day_key] = {
            "impressions": day["impressions"],
            "clicks": day["clicks"],
            "purchases": day["purchases"],
            "revenue": round(day["revenue"], 2),
            "conversions": day["conversions"],
            "attrRevenue": round(day["attrRevenue"], 2),
            "newOrders": day["newOrders"],
            "returningOrders": day["returningOrders"],
            "nyPurchases": day["nyPurchases"],
            "trafficSources": dict(day["trafficSources"]),
        }

    # -------------------------
    # Lookback data
    # -------------------------
    lookback_windows = [1, 3, 7, 14, 30]
    lookback_data = {}
    for lb_days in lookback_windows:
        lb_orders = 0
        lb_revenue = 0.0
        for a in unique_attributed:
            imp_time_str = a.get("impressionTime")
            conv_time_str = a.get("time")
            if not imp_time_str or not conv_time_str:
                lb_orders += 1
                lb_revenue += float(a.get("revenue") or 0)
                continue
            try:
                imp_time = pd.to_datetime(imp_time_str, utc=True)
                conv_time = pd.to_datetime(conv_time_str, utc=True)
                delta_days = (conv_time - imp_time).total_seconds() / 86400
            except Exception:
                lb_orders += 1
                lb_revenue += float(a.get("revenue") or 0)
                continue

            if delta_days <= lb_days:
                lb_orders += 1
                lb_revenue += float(a.get("revenue") or 0)

        lookback_data[str(lb_days)] = {
            "orders": lb_orders,
            "revenue": round(lb_revenue, 2),
        }

    # -------------------------
    # Build traffic source detail
    # -------------------------
    traffic_source_detail = {}
    for source, count in traffic_counts.most_common():
        traffic_source_detail[source] = {
            "orders": count,
            "revenue": round(traffic_revenue[source], 2),
            "aov": round(traffic_revenue[source] / count, 2) if count > 0 else 0,
            "pctOfOrders": round(count / total_orders * 100, 1) if total_orders > 0 else 0,
        }

    # -------------------------
    # Build geographic detail
    # -------------------------
    geo_detail = {}
    for region, count in geo_region.most_common(25):
        geo_detail[region] = {
            "orders": count,
            "revenue": round(geo_revenue[region], 2),
        }

    # -------------------------
    # Build device detail
    # -------------------------
    device_detail = {}
    for device, count in device_counts.most_common():
        device_detail[device] = {
            "orders": count,
            "pctOfOrders": round(count / total_orders * 100, 1) if total_orders > 0 else 0,
        }

    # -------------------------
    # Build campaign-level data (if programmatic exists)
    # -------------------------
    campaigns_output = {}
    if campaign_info:
        for cid, cinfo in campaign_info.items():
            camp_imps_with_pid = [(pid, n) for c, pid, n in all_impressions if c == cid]
            camp_clicks = [n for c, _, n in all_clicks_clean if c == cid]
            camp_attr = [a for a in unique_attributed if a.get("impressionCampaignId") == cid]

            camp_imps = [n for _, n in camp_imps_with_pid]
            c_imp_count = len(camp_imps)
            c_click_count = len(camp_clicks)
            c_attr_count = len(camp_attr)
            c_attr_revenue = sum(float(a.get("revenue") or 0) for a in camp_attr)
            c_reach = len(set(n.get("ip") for n in camp_imps if n.get("ip")))
            c_frequency = round(c_imp_count / c_reach, 2) if c_reach > 0 else 0
            c_spend = 0
            for pid, n in camp_imps_with_pid:
                cpm = PLACEMENT_CONFIG.get(pid, {}).get("cpm", DEFAULT_CPM)
                c_spend += cpm / 1000
            c_spend = round(c_spend, 2)
            c_roas = round(c_attr_revenue / c_spend, 2) if c_spend > 0 else 0

            campaigns_output[f"campaign_{cid}"] = {
                "label": cinfo["label"],
                "campaignId": cid,
                "campaignName": cinfo["name"],
                "impressions": c_imp_count,
                "clicks": c_click_count,
                "reach": c_reach,
                "frequency": c_frequency,
                "attrOrders": c_attr_count,
                "attrRevenue": round(c_attr_revenue, 2),
                "spend": c_spend,
                "roas": c_roas,
            }

    # -------------------------
    # Build channel breakdown from placement data
    # -------------------------
    channel_data = {}
    if has_programmatic:
        channel_imps = defaultdict(list)   # channel -> [(pid, node)]
        channel_clicks = defaultdict(list)  # channel -> [node]
        for _, pid, n in all_impressions:
            if id(n) in clean_imp_ids:
                ch = PLACEMENT_CONFIG.get(pid, {}).get("channel", "display")
                channel_imps[ch].append((pid, n))
        for _, pid, n in all_clicks_clean:
            ch = PLACEMENT_CONFIG.get(pid, {}).get("channel", "display")
            channel_clicks[ch].append(n)

        # Map attributed conversions to channels via impressionPlacementId or fallback
        channel_attr = defaultdict(list)
        for a in unique_attributed:
            a_pid = a.get("impressionPlacementId") or a.get("placementId", "")
            ch = PLACEMENT_CONFIG.get(a_pid, {}).get("channel", "display")
            channel_attr[ch].append(a)

        for ch_name in ["display", "video", "ctv"]:
            ch_imps = channel_imps.get(ch_name, [])
            ch_clks = channel_clicks.get(ch_name, [])
            ch_attrs = channel_attr.get(ch_name, [])
            ch_imp_count = len(ch_imps)
            ch_click_count = len(ch_clks)
            ch_reach = len(set(n.get("ip") for _, n in ch_imps if n.get("ip")))
            ch_attr_count = len(ch_attrs)
            ch_attr_revenue = round(sum(float(a.get("revenue") or 0) for a in ch_attrs), 2)
            ch_cpm = PLACEMENT_CONFIG.get(
                next((pid for pid, _ in ch_imps), ""), {}
            ).get("cpm", DEFAULT_CPM)
            ch_spend = round(ch_imp_count / 1000 * ch_cpm, 2)
            ch_roas = round(ch_attr_revenue / ch_spend, 2) if ch_spend > 0 else 0

            channel_data[ch_name] = {
                "impressions": ch_imp_count,
                "clicks": ch_click_count,
                "reach": ch_reach,
                "attrOrders": ch_attr_count,
                "attrRevenue": ch_attr_revenue,
                "cpm": ch_cpm,
                "spend": ch_spend,
                "roas": ch_roas,
            }

        print(f"\n  Channel breakdown:")
        for ch_name, ch in channel_data.items():
            print(f"    {ch_name}: {ch['impressions']:,} imp, ${ch['cpm']:.2f} CPM, ${ch['spend']:.2f} spend, {ch['attrOrders']} attr orders, ROAS {ch['roas']:.2f}x")

    # -------------------------
    # Assemble final JSON
    # -------------------------
    output = {
        "version": 2,
        "advertiserId": ADVERTISER_ID,
        "advertiserName": "Purple Carrot",
        "model": "DTC Subscription",
        "tagVersion": "v14.10.0",
        "lastUpdated": datetime.now(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
        "dateRange": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": run_end_date.strftime("%Y-%m-%d"),
        },
        "global": {
            "totalOrders": total_orders,
            "totalRevenue": round(total_revenue, 2),
            "aov": aov,
            "uniqueVisitors": total_unique_visitors,
            "repeatPurchaseRate": repeat_rate,
            "newOrders": new_orders,
            "returningOrders": returning_orders,
            "newCustomerPct": new_pct,
            "avgCartSize": avg_cart_size,
        },
        "identity": {
            "emailHashCoverage": email_coverage,
            "customerIdCoverage": customer_id_coverage,
            "zipCodeCoverage": zip_coverage,
            "loggedInBreakdown": {
                "yes": logged_in_counts.get("yes", 0),
                "no": logged_in_counts.get("no", 0),
                "unknown": logged_in_counts.get("unknown", 0),
            },
        },
        "orderSources": {
            "ordersuccess": source_counts.get("ordersuccess", 0),
            "cartFallback": source_counts.get("cart_fallback", 0),
            "unknown": source_counts.get("unknown", 0),
        },
        "trafficSources": traffic_source_detail,
        "geographic": geo_detail,
        "devices": device_detail,
        "programmatic": {
            "enabled": has_programmatic,
            "impressions": imp_count,
            "clicks": click_count,
            "reach": prog_reach,
            "frequency": prog_frequency,
            "attrOrders": attr_count,
            "attrRevenue": round(attr_revenue, 2),
            "spend": prog_spend,
            "roas": prog_roas,
            "costPerSubscription": prog_cpa,
            "ctVisits": ct_unique_visitors,
            "vtVisits": vt_unique_visitors,
            "stackadapt": {
                "convPixel": SAQ_CONV_KEY,
                "rtPixel": SAQ_RT_SID,
            },
        },
        "attribution": {
            "byType": {
                "clickThrough": attr_click_through,
                "viewThrough": attr_view_through,
            },
            "conversionTimeline": timeline,
            "lookbackData": lookback_data,
            "avgDaysToConversion": avg_days_to_conv,
            "sameDayConversions": same_day_count,
            "sameDayConversionRate": round(same_day_count / attr_count * 100, 1) if attr_count > 0 else 0,
            "impressionsToConversion": {
                "avgImpressions": avg_imps_to_convert,
                "minImpressions": min_imps_to_convert,
                "maxImpressions": max_imps_to_convert,
                "medianImpressions": median_imps_to_convert,
                "frequencyBuckets": freq_buckets,
                "converterDetails": converter_details,
            },
        },
        "channels": channel_data,
        "campaigns": campaigns_output,
        "dailyData": serialized_daily,
    }

    # NOTE: Attribution preservation removed — daily conversions now always reflect
    # the current dedup pipeline (ordersuccess-only + visitor-window dedup).
    # Previously this kept higher historical values, but that conflicted with
    # the stricter dedup logic.

    # -------------------------
    # Write output
    # -------------------------
    v2_path = os.path.join(DATA_DIR, "dashboard_data_v2.json")
    with open(v2_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nv2 JSON written: {v2_path}")

    # -------------------------
    # Generate CSV exports
    # -------------------------
    import csv
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Daily summary CSV
    daily_csv_name = f"purplecarrot_daily_summary_{ts}.csv"
    daily_csv_path = os.path.join(DATA_DIR, daily_csv_name)
    with open(daily_csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "purchases", "revenue", "aov",
            "new_orders", "returning_orders",
            "impressions", "clicks", "attributed_orders", "attributed_revenue"
        ])
        for day_key in sorted(output["dailyData"].keys()):
            d = output["dailyData"][day_key]
            day_orders = d.get("purchases", 0)
            day_rev = d.get("revenue", 0)
            day_aov = round(day_rev / day_orders, 2) if day_orders > 0 else 0
            writer.writerow([
                day_key, day_orders, round(day_rev, 2), day_aov,
                d.get("newOrders", 0), d.get("returningOrders", 0),
                d.get("impressions", 0), d.get("clicks", 0),
                d.get("conversions", 0), round(d.get("attrRevenue", 0), 2),
            ])
    print(f"CSV written: {daily_csv_path}")

    # Orders detail CSV
    orders_csv_name = f"purplecarrot_orders_{ts}.csv"
    orders_csv_path = os.path.join(DATA_DIR, orders_csv_name)
    with open(orders_csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_time_est", "dedupe_id", "order_source", "revenue", "cart_qty",
            "visitor_id", "visitor_status", "logged_in",
            "email_hash", "customer_id", "zip_code",
            "traffic_source", "utm_source", "utm_medium", "utm_campaign",
            "device_os", "country", "region", "city",
            "referrer", "event_url"
        ])
        for p in unique_purchases:
            t = p.get("time", "")
            try:
                dt = pd.to_datetime(t, utc=True).astimezone(LOCAL_TZ)
                time_est = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                time_est = t[:19] if t else ""
            utms = p.get("_utms", {})
            writer.writerow([
                time_est,
                p.get("dedupeId", ""),
                p.get("_order_source", ""),
                round(float(p.get("revenue") or 0), 2),
                p.get("cartQty", ""),
                p.get("visitorId", ""),
                p.get("visitorStatus", ""),
                p.get("_logged_in", ""),
                p.get("_email_hash", "")[:16] + "..." if p.get("_email_hash") else "",
                p.get("_customer_id", ""),
                p.get("_zip_code", ""),
                p.get("_traffic_source", ""),
                utms.get("utm_source", ""),
                utms.get("utm_medium", ""),
                utms.get("utm_campaign", ""),
                p.get("osName", ""),
                p.get("countryCode", ""),
                p.get("region", ""),
                p.get("city", ""),
                (p.get("referrer") or "")[:100],
                (p.get("url") or "")[:150],
            ])
    print(f"CSV written: {orders_csv_path}")

    # Traffic source CSV
    traffic_csv_name = f"purplecarrot_traffic_sources_{ts}.csv"
    traffic_csv_path = os.path.join(DATA_DIR, traffic_csv_name)
    with open(traffic_csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["traffic_source", "orders", "revenue", "aov", "pct_of_orders"])
        for source, detail in sorted(traffic_source_detail.items(), key=lambda x: x[1]["orders"], reverse=True):
            writer.writerow([
                source, detail["orders"], detail["revenue"],
                detail["aov"], detail["pctOfOrders"],
            ])
    print(f"CSV written: {traffic_csv_path}")

    # Cache files index
    cache_files = {
        "daily_summary": daily_csv_name,
        "orders_detail": orders_csv_name,
        "traffic_sources": traffic_csv_name,
    }
    cache_files_path = os.path.join(DATA_DIR, "dashboard_cache_files.json")
    with open(cache_files_path, "w") as f:
        json.dump(cache_files, f, indent=2)
    print(f"Cache files index written: {cache_files_path}")

    # Upload to web service if on Render
    if os.environ.get("RENDER") and DASHBOARD_URL:
        print("\n[Upload] Sending v2 cache to web service...")
        try:
            upload_resp = requests.post(
                f"{DASHBOARD_URL}/upload_cache_v2",
                json=output,
                headers={"X-Upload-Secret": UPLOAD_SECRET},
                timeout=30
            )
            if upload_resp.status_code == 200:
                print("[Upload] v2 SUCCESS")
            else:
                print(f"[Upload] v2 FAILED - {upload_resp.status_code}: {upload_resp.text}")
        except Exception as e:
            print(f"[Upload] v2 ERROR: {e}")

    # -------------------------
    # Print summary
    # -------------------------
    print(f"\n{'=' * 70}")
    print("PURPLE CARROT — SUMMARY")
    print(f"{'=' * 70}")
    print(f"Date Range: {run_start_date} to {run_end_date}")
    print(f"\n--- Purchase Analytics ---")
    print(f"Total Orders:     {total_orders:,}")
    print(f"Total Revenue:    ${total_revenue:,.2f}")
    print(f"AOV:              ${aov:,.2f}")
    print(f"Unique Visitors:  {total_unique_visitors:,}")
    print(f"Repeat Rate:      {repeat_rate}%")
    print(f"Avg Cart Size:    {avg_cart_size}")
    print(f"\n--- Customer Type ---")
    print(f"New:              {new_orders:,} ({new_pct}%)")
    print(f"Returning:        {returning_orders:,}")
    print(f"Logged In:        {logged_in_counts.get('yes', 0):,} / Not: {logged_in_counts.get('no', 0):,}")
    print(f"\n--- Order Sources ---")
    print(f"ordersuccess:     {source_counts.get('ordersuccess', 0):,}")
    print(f"cart fallback:    {source_counts.get('cart_fallback', 0):,}")
    print(f"\n--- Identity Coverage ---")
    print(f"Email Hash:       {email_coverage}%")
    print(f"Customer ID:      {customer_id_coverage}%")
    print(f"Zip Code:         {zip_coverage}%")
    print(f"\n--- Traffic Sources ---")
    for source, count in traffic_counts.most_common(10):
        rev = traffic_revenue[source]
        print(f"  {source:<25} {count:>5} orders  ${rev:>10,.2f}")
    print(f"\n--- Top Regions ---")
    for region, count in geo_region.most_common(10):
        print(f"  {region:<25} {count:>5} orders  ${geo_revenue[region]:>10,.2f}")
    print(f"\n--- Devices ---")
    for device, count in device_counts.most_common():
        print(f"  {device:<25} {count:>5} orders")
    if has_programmatic:
        print(f"\n--- Programmatic (StackAdapt) ---")
        print(f"Impressions:      {imp_count:,}")
        print(f"Clicks:           {click_count:,}")
        print(f"Reach:            {prog_reach:,}")
        print(f"Frequency:        {prog_frequency}")
        print(f"Attributed:       {attr_count:,} orders, ${attr_revenue:,.2f} revenue")
        print(f"Spend (est):      ${prog_spend:,.2f}")
        print(f"ROAS:             {prog_roas}x")
        print(f"Cost/Subscription:${prog_cpa:,.2f}")
        print(f"CT vs VT:         {attr_click_through} click-through, {attr_view_through} view-through")
        print(f"Avg Days to Conv: {avg_days_to_conv}")
    else:
        print(f"\n--- Programmatic ---")
        print(f"No impression/click data yet. StackAdapt RT pixel ({SAQ_RT_SID}) active.")
        print(f"Attributed orders will appear once programmatic campaigns are live.")

    print(f"\n{'=' * 70}")
    print("Done!")


if __name__ == "__main__":
    main()
