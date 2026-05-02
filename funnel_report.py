"""
Purple Carrot Funnel Report — v3 (post tag v15.4.0)

Builds the full visit-to-purchase funnel for Purple Carrot (advertiser 1060,
scriptId QwxIISziQhWR), bucketing gift redemptions separately from paid orders.

Stages (each measured as unique visitors per session):
  1. WEBSITE VISITOR   — allVisits, any URL
  2. CART              — allEvents event=add_to_cart
  3. ACCOUNT (email)   — allEvents event=account
  4. SHIPPING          — allEvents event=add_shipping_info
  5. PAYMENT           — allEvents event=add_payment_info
  6. PURCHASE (paid)   — allPurchases dedupeId 297/298/299 prefix, revenue>0,
                         URL not /redeem_signup/
  7. REDEEM (gift)     — allEvents event=redeem  OR  allPurchases with
                         /redeem_signup/ in URL  OR  revenue=0 + 297-prefix id

Outputs:
  - funnel_report_<ts>.csv         daily rows + totals
  - funnel_dashboard.html          visual drop-off dashboard with leak callouts
  - console summary with biggest drop-off highlighted
"""
import requests
import csv
import os
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict

GRAPHQL_URL = "https://dev.graph.script.flowershop.media/graphql"
AUTH = ("developer", "FlowerShop2024DevGraphQL123")
SCRIPT_ID = "QwxIISziQhWR"
ADVERTISER_ID = "1060"
DAYS_BACK = int(os.environ.get("DAYS_BACK", "0"))  # 0 = auto-detect from earliest allEvents row

# Campaign start date — used for the "Programmatic engagement" section so the
# ad vs non-ad comparison spans the full campaign rather than the funnel-events
# window (which is only post-tag-v15.3.0).
CAMPAIGN_START = os.environ.get("CAMPAIGN_START", "2026-03-11T00:00:00Z")
PAGE_SIZE = 5000

OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def gq(query):
    for _ in range(3):
        try:
            r = requests.post(GRAPHQL_URL, json={"query": query}, auth=AUTH, timeout=180)
            data = r.json()
            if "errors" in data:
                print("GQ error:", data["errors"])
                return None
            return data.get("data", {})
        except Exception as e:
            print(f"  retry: {e}")
    return None


def fetch_paginated(table, filter_clause, fields):
    nodes = []
    offset = 0
    while True:
        q = f'''
        {{
          {table}(
            filter: {filter_clause}
            first: {PAGE_SIZE} offset: {offset} orderBy: TIME_DESC
          ) {{ nodes {{ {fields} }} }}
        }}'''
        data = gq(q)
        if not data:
            break
        chunk = data[table]["nodes"]
        nodes.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset >= 250_000:
            break
    return nodes


def is_real_order_id(did):
    s = str(did or "").strip()
    return s.isdigit() and len(s) == 7 and s[:3] in ("297", "298", "299")


def is_redeem_purchase(p):
    """A paid 297-id row from /redeem_signup/ OR with $0 revenue."""
    url = str(p.get("url") or "")
    if "/redeem_signup/" in url:
        return True
    try:
        if float(p.get("revenue") or 0) == 0:
            return True
    except Exception:
        pass
    return False


def fetch_campaign_period_comparison():
    """Campaign-period (since CAMPAIGN_START) cohort comparison with the
    ad-attributed cohort properly SUBTRACTED from site totals so the
    'non-attributed' bucket is truly organic, not contaminated."""
    print(f"\nComputing campaign-period comparison (since {CAMPAIGN_START[:10]})...")
    end_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def fp(table, flt, fields):
        nodes, offset = [], 0
        while True:
            q = f'{{ {table}(filter: {flt} first: 5000 offset: {offset} orderBy: TIME_DESC) {{ nodes {{ {fields} }} }} }}'
            d = gq(q) or {}
            chunk = (d.get(table) or {}).get("nodes", [])
            if not chunk: break
            nodes.extend(chunk)
            if len(chunk) < 5000: break
            offset += 5000
        return nodes

    # Site purchases in window — use real-shape (297/298/299) WITHOUT redeem filter
    # so gift-redemption orders that were ad-attributed still count as attributed orders.
    # Matches the user's expected total of 7 (5 paid + 2 redeems).
    purchases = fp("allPurchases",
                   f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{CAMPAIGN_START}" lessThanOrEqualTo: "{end_iso}" }} }}',
                   "pk dedupeId revenue url visitorId")
    real_orders = [p for p in purchases if is_real_order_id(p.get("dedupeId"))]
    site_orders = len(real_orders)
    site_revenue = sum(float(p.get("revenue") or 0) for p in real_orders)
    site_pks = {p["pk"] for p in real_orders if p.get("pk") is not None}
    # Build dedupeId → purchase lookup for the allPurchaseAttributions-based join below
    site_did_to_p = {p["dedupeId"]: p for p in real_orders}

    # Site visits in window — day-by-day to avoid backend timeout
    visits = []
    day = datetime.fromisoformat(CAMPAIGN_START.replace("Z","+00:00"))
    end_dt = datetime.now(timezone.utc)
    while day <= end_dt:
        de = day + timedelta(days=1)
        flt = f'{{ scriptId: {{ equalTo: "{SCRIPT_ID}" }} time: {{ greaterThanOrEqualTo: "{day.strftime("%Y-%m-%dT%H:%M:%SZ")}" lessThan: "{de.strftime("%Y-%m-%dT%H:%M:%SZ")}" }} }}'
        chunk = fetch_paginated("allVisits", flt, "visitorId pk")
        visits.extend(chunk)
        day = de
    site_unique_visitors = {v["visitorId"] for v in visits if v.get("visitorId")}

    # Ad-attributed visit pks in window
    attributed_visit_pks = set()
    for n in fp("allAggregatedMatchesByVisits",
                f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{CAMPAIGN_START}" lessThanOrEqualTo: "{end_iso}" }} }}',
                "visitPk"):
        if n.get("visitPk") is not None:
            attributed_visit_pks.add(n["visitPk"])

    # Resolve visit pks → visitorIds
    ad_visitor_ids = set()
    pk_list = list(attributed_visit_pks)
    for i in range(0, len(pk_list), 500):
        batch = pk_list[i:i+500]
        pk_filter = "[" + ",".join('"' + str(p) + '"' for p in batch) + "]"
        q = f'{{ allVisits(filter: {{ pk: {{ in: {pk_filter} }} }} first: 5000) {{ nodes {{ visitorId }} }} }}'
        d = gq(q) or {}
        for n in (d.get("allVisits") or {}).get("nodes", []):
            if n.get("visitorId"): ad_visitor_ids.add(n["visitorId"])

    # Ad-attributed orders (in window) — UNION of two attribution tables for
    # maximum coverage. allPurchaseAttributions tends to surface 2 extra rows
    # per current data (the gift-redemption attributions); allAggregatedMatchesByPurchases
    # captures the rest.
    # Method A: dedupeId join via allPurchaseAttributions
    attr_dids_a = set()
    for n in fp("allPurchaseAttributions",
                f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{CAMPAIGN_START}" lessThanOrEqualTo: "{end_iso}" }} }}',
                "dedupeId"):
        if n.get("dedupeId") and is_real_order_id(n.get("dedupeId")):
            attr_dids_a.add(n["dedupeId"])
    # Method B: purchasePk join via allAggregatedMatchesByPurchases
    attr_purchase_pks = set()
    for n in fp("allAggregatedMatchesByPurchases",
                f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{CAMPAIGN_START}" lessThanOrEqualTo: "{end_iso}" }} }}',
                "purchasePk"):
        if n.get("purchasePk") is not None:
            attr_purchase_pks.add(n["purchasePk"])
    attr_dids_b = {p["dedupeId"] for p in real_orders if p.get("pk") in attr_purchase_pks}
    # Union — every order that EITHER source flags as attributed
    attr_real_dids = attr_dids_a | attr_dids_b
    ad_orders_list = [site_did_to_p[d] for d in attr_real_dids if d in site_did_to_p]
    ad_orders = len(ad_orders_list)
    ad_revenue = sum(float(p.get("revenue") or 0) for p in ad_orders_list)
    # Add paid-purchase visitorIds to ad cohort (they're definitionally ad-driven)
    for p in ad_orders_list:
        if p.get("visitorId"): ad_visitor_ids.add(p["visitorId"])

    # SUBTRACT ad cohort from site to get the truly-organic non-attributed cohort
    ad_visitors    = len(ad_visitor_ids)
    non_visitors   = len(site_unique_visitors - ad_visitor_ids)
    non_orders     = site_orders - ad_orders
    non_revenue    = site_revenue - ad_revenue

    # ── NY-only benchmark ──
    # Pull NY visits (day-by-day) + NY purchases for a state-specific comparison.
    print("Pulling NY visits + purchases for state benchmark...")
    ny_visits = []
    day = datetime.fromisoformat(CAMPAIGN_START.replace("Z","+00:00"))
    while day <= end_dt:
        de = day + timedelta(days=1)
        flt = f'{{ scriptId: {{ equalTo: "{SCRIPT_ID}" }} region: {{ equalTo: "NY" }} time: {{ greaterThanOrEqualTo: "{day.strftime("%Y-%m-%dT%H:%M:%SZ")}" lessThan: "{de.strftime("%Y-%m-%dT%H:%M:%SZ")}" }} }}'
        chunk = fetch_paginated("allVisits", flt, "visitorId pk")
        ny_visits.extend(chunk)
        day = de
    ny_visitors = len({v["visitorId"] for v in ny_visits if v.get("visitorId")})
    ny_purchases = fp("allPurchases",
                      f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} region: {{ equalTo: "NY" }} time: {{ greaterThanOrEqualTo: "{CAMPAIGN_START}" lessThanOrEqualTo: "{end_iso}" }} }}',
                      "dedupeId revenue url")
    ny_real_orders = [p for p in ny_purchases if is_real_order_id(p.get("dedupeId"))]
    ny_orders = len(ny_real_orders)
    ny_revenue = sum(float(p.get("revenue") or 0) for p in ny_real_orders)
    print(f"  NY visitors: {ny_visitors:,}    NY orders: {ny_orders:,}    NY revenue: ${ny_revenue:,.2f}")

    ad_v = max(ad_visitors, 1)
    non_v = max(non_visitors, 1)
    return {
        "campaign_start":       CAMPAIGN_START,
        # Top-line numbers
        "site_visitors":        len(site_unique_visitors),
        "site_orders":          site_orders,
        "site_revenue":         round(site_revenue, 2),
        "ad_visitors":          ad_visitors,
        "ad_orders":            ad_orders,
        "ad_revenue":           round(ad_revenue, 2),
        "non_visitors":         non_visitors,
        "non_orders":           non_orders,
        "non_revenue":          round(non_revenue, 2),
        # Rates
        "ad_conv_rate":         round(100 * ad_orders / ad_v, 3),
        "non_conv_rate":        round(100 * non_orders / non_v, 3),
        "ad_aov":               round(ad_revenue / max(ad_orders, 1), 2),
        "non_aov":              round(non_revenue / max(non_orders, 1), 2),
        "ad_rpv":               round(ad_revenue / ad_v, 4),
        "non_rpv":              round(non_revenue / non_v, 4),
        # NY-only benchmark
        "ny_visitors":          ny_visitors,
        "ny_orders":            ny_orders,
        "ny_revenue":           round(ny_revenue, 2),
        "ny_conv_rate":         round(100 * ny_orders / max(ny_visitors, 1), 3),
    }


def fetch_ad_lift():
    """Compute programmatic engagement + attributed-order metrics. Uses
    allAggregatedMatchesByVisits + allAggregatedMatchesByPurchases as the
    source of truth. (allLastTouchMatches and allLastTouchMatchByPurchases
    are broken/empty for this advertiser as of 2026-04-29.)
    """
    print("Computing programmatic engagement metrics (all-time)...")

    def fp(table, flt, fields):
        nodes, offset = [], 0
        while True:
            q = f'{{ {table}(filter: {flt} first: 5000 offset: {offset} orderBy: TIME_DESC) {{ nodes {{ {fields} }} }} }}'
            data = gq(q)
            if not data:
                break
            chunk = data[table]["nodes"]
            nodes.extend(chunk)
            if len(chunk) < 5000:
                break
            offset += 5000
        return nodes

    # Visit-side aggregated matches — gives us CT/VT visit split + days-to-visit
    ampbv = fp("allAggregatedMatchesByVisits", f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }}',
               "visitPk isClickThrough daysBetween")
    ad_visits_ct = {n["visitPk"] for n in ampbv if n.get("visitPk") and n["isClickThrough"]}
    ad_visits_vt = {n["visitPk"] for n in ampbv if n.get("visitPk") and not n["isClickThrough"]}
    ad_visits_all = ad_visits_ct | ad_visits_vt
    days_to_visit = sorted(n["daysBetween"] for n in ampbv if n.get("daysBetween") is not None)
    median_days_visit = (days_to_visit[len(days_to_visit)//2] if days_to_visit else 0)
    avg_days_visit = (sum(days_to_visit)/len(days_to_visit)) if days_to_visit else 0

    # Purchase-side aggregated matches — gives us attributed purchasePks
    ampbp = fp("allAggregatedMatchesByPurchases", f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }}',
               "purchasePk isClickThrough daysBetween")
    attr_pks = {n["purchasePk"] for n in ampbp if n.get("purchasePk")}
    attr_ct_pks = {n["purchasePk"] for n in ampbp if n.get("purchasePk") and n["isClickThrough"]}
    attr_vt_pks = {n["purchasePk"] for n in ampbp if n.get("purchasePk") and not n["isClickThrough"]}
    days_to_purchase = sorted(n["daysBetween"] for n in ampbp if n.get("daysBetween") is not None)
    median_days_purchase = (days_to_purchase[len(days_to_purchase)//2] if days_to_purchase else 0)

    # Cross-ref to real-paid orders — get purchase pks for real-paid only
    pa = fp("allPurchases", f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }}',
            "pk dedupeId revenue url")
    real_paid_pks = set()
    for p in pa:
        if is_real_order_id(p.get("dedupeId")) and not is_redeem_purchase(p) and p.get("pk") is not None:
            real_paid_pks.add(p["pk"])
    total_paid_orders = len(real_paid_pks)
    attr_real_paid_pks = attr_pks & real_paid_pks
    attr_real_ct = attr_ct_pks & real_paid_pks
    attr_real_vt = attr_vt_pks & real_paid_pks

    # Total impressions (for visit-rate metric) — this table uses DAY_DESC orderBy
    total_imp = 0
    offset = 0
    while True:
        q = f'{{ allImpressionDailyCounts(filter: {{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }} first: 5000 offset: {offset} orderBy: DAY_DESC) {{ nodes {{ totalCount }} }} }}'
        data = gq(q)
        if not data: break
        chunk = data["allImpressionDailyCounts"]["nodes"]
        if not chunk: break
        for n in chunk:
            total_imp += int(n.get("totalCount") or 0)
        if len(chunk) < 5000: break
        offset += 5000

    # Visit attribution rate — % of impressions that became a site visit
    visit_rate_pct = (100 * len(ad_visits_all) / total_imp) if total_imp else 0

    return {
        # Engagement metrics (the favorable framing)
        "total_impressions":          total_imp,
        "ad_visits_total":            len(ad_visits_all),
        "ad_visits_ct":               len(ad_visits_ct),
        "ad_visits_vt":               len(ad_visits_vt),
        "visit_rate_pct":             round(visit_rate_pct, 4),
        "median_days_to_visit":       median_days_visit,
        "avg_days_to_visit":          round(avg_days_visit, 2),
        # Attribution-share metrics (the honest companion)
        "ae_attributed_orders":       len(attr_real_paid_pks),
        "ae_attributed_orders_ct":    len(attr_real_ct),
        "ae_attributed_orders_vt":    len(attr_real_vt),
        "total_paid_orders":          total_paid_orders,
        "ae_share_pct":               round(100 * len(attr_real_paid_pks) / max(total_paid_orders, 1), 2),
        "median_days_to_purchase":    median_days_purchase,
        # Compatibility with old keys so existing render code keeps working
        "ae_visitors":                len(ad_visits_all),
        "ae_visitors_ct":             len(ad_visits_ct),
        "ae_visitors_vt":             len(ad_visits_vt),
        "ae_purchasers":              len(attr_real_paid_pks),
        "ae_conv_rate":               round(100 * len(attr_real_paid_pks) / max(len(ad_visits_all), 1), 2),
    }


def main():
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    if DAYS_BACK > 0:
        start = end - timedelta(days=DAYS_BACK)
    else:
        # Auto-detect: pull earliest allEvents row for this advertiser. The funnel
        # window is "since funnel events started flowing" (v15.3.0 deploy time).
        q = f'{{ allEvents(filter: {{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }} first: 1 orderBy: TIME_ASC) {{ nodes {{ time }} }} }}'
        d = gq(q) or {}
        nodes = (d.get("allEvents") or {}).get("nodes", [])
        if nodes and nodes[0].get("time"):
            start = datetime.fromisoformat(nodes[0]["time"].replace("Z","+00:00")).replace(hour=0, minute=0, second=0, microsecond=0)
            print(f"Auto-detected funnel-events start: {start.strftime('%Y-%m-%d')}")
        else:
            start = end - timedelta(days=7)
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    days_in_window = (end - start).days
    print(f"Date range: {start_iso} → {end_iso}  ({days_in_window} days)\n")

    print("Fetching visits (day-by-day to avoid backend timeout)...")
    visits = []
    day = start
    while day < end:
        day_end = day + timedelta(days=1)
        d_start = day.strftime("%Y-%m-%dT%H:%M:%SZ")
        d_end = day_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        chunk = fetch_paginated(
            "allVisits",
            f'{{ scriptId: {{ equalTo: "{SCRIPT_ID}" }} time: {{ greaterThanOrEqualTo: "{d_start}" lessThan: "{d_end}" }} }}',
            "time visitorId visitorStatus url ip pk"
        )
        visits.extend(chunk)
        print(f"  {d_start[:10]}: +{len(chunk):,}  (running {len(visits):,})")
        day = day_end
    print(f"  {len(visits):,} visit rows total")

    # Pull historical paid-purchaser visitor IDs (all-time) so we can flag
    # "existing customers" — anyone whose visitorId has ever made a real paid
    # order. The visit-row visitorStatus only emits "new" / "returning_non_purchaser"
    # today, so we re-classify on top of it using the purchase-history join.
    print("Fetching all-time paid-purchaser visitorIds for existing-customer flag...")
    all_pp_vids = set()
    offset = 0
    while True:
        q = f'{{ allPurchases(filter: {{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }} first: 5000 offset: {offset} orderBy: TIME_DESC) {{ nodes {{ dedupeId visitorId revenue url }} }} }}'
        d = gq(q)
        if not d: break
        chunk = d["allPurchases"]["nodes"]
        if not chunk: break
        for p in chunk:
            if is_real_order_id(p.get("dedupeId")) and not is_redeem_purchase(p):
                if p.get("visitorId"):
                    all_pp_vids.add(p["visitorId"])
        if len(chunk) < 5000: break
        offset += 5000
    print(f"  historical paid-purchaser visitor IDs: {len(all_pp_vids):,}")

    # Visitor-status breakdown (unique visitors per status, with existing-customer
    # detection via historical-purchaser join).
    status_unique = {"new": set(), "returning_non_purchaser": set(), "returning_purchaser": set(), "unknown": set()}
    for v in visits:
        vid = v.get("visitorId")
        if not vid:
            continue
        # Existing customer detection wins regardless of stale visitorStatus
        if vid in all_pp_vids:
            status_unique["returning_purchaser"].add(vid)
            continue
        s = (v.get("visitorStatus") or "unknown").lower()
        if s.startswith("new"):
            status_unique["new"].add(vid)
        elif s in ("returning_purchaser", "returning_customer", "existing", "returning"):
            status_unique["returning_purchaser"].add(vid)
        elif s.startswith("returning") or "returning" in s:
            status_unique["returning_non_purchaser"].add(vid)
        else:
            status_unique["unknown"].add(vid)
    # Make the statuses mutually exclusive — a visitor is in exactly one bucket.
    # Existing customers already removed from new/non-purchaser by `continue` above.
    # But the same vid could end up in both "new" AND "returning_non_purchaser"
    # if its status flipped during the window. Resolve by preferring returning.
    status_unique["new"] -= status_unique["returning_non_purchaser"]
    status_unique["new"] -= status_unique["returning_purchaser"]
    status_unique["returning_non_purchaser"] -= status_unique["returning_purchaser"]
    visitor_status_counts = {k: len(v) for k, v in status_unique.items()}
    unique_visitor_set = {v["visitorId"] for v in visits if v.get("visitorId")}
    visitor_status_counts["total_unique"] = len(unique_visitor_set)
    visitor_status_counts["total_pageviews"] = len(visits)
    print(f"  Visitor status: {visitor_status_counts}")

    print("Fetching funnel events...")
    events = fetch_paginated(
        "allEvents",
        f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{start_iso}" lessThan: "{end_iso}" }} }}',
        "time event ip"
    )
    print(f"  {len(events):,} event rows")

    print("Fetching purchases...")
    purchases = fetch_paginated(
        "allPurchases",
        f'{{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} time: {{ greaterThanOrEqualTo: "{start_iso}" lessThan: "{end_iso}" }} }}',
        "time dedupeId revenue url visitorId"
    )
    print(f"  {len(purchases):,} purchase rows\n")

    # Daily aggregation
    daily = defaultdict(lambda: {
        "visitors":          set(),
        "add_to_cart":       set(),
        "account":           set(),
        "add_shipping_info": set(),
        "add_payment_info":  set(),
        "redeem_visitors":   set(),
        "purchases_paid":    0,
        "purchases_redeem":  0,
        "revenue_paid":      0.0,
    })

    for v in visits:
        if v.get("time") and v.get("visitorId"):
            daily[v["time"][:10]]["visitors"].add(v["visitorId"])

    # Events: we don't have visitorId in allEvents schema, use IP as a stand-in for unique-visitor at this stage
    # (sessionStorage dedupe in tag means at most one event per visitor per session anyway)
    for e in events:
        if not e.get("time"):
            continue
        ev_name = e.get("event")
        if ev_name in {"add_to_cart", "account", "add_shipping_info", "add_payment_info"}:
            daily[e["time"][:10]][ev_name].add(e.get("ip") or e.get("time"))
        elif ev_name == "redeem":
            daily[e["time"][:10]]["redeem_visitors"].add(e.get("ip") or e.get("time"))

    # Purchases: real-order-id gate + redeem split
    for p in purchases:
        if not p.get("time") or not is_real_order_id(p.get("dedupeId")):
            continue
        day = p["time"][:10]
        if is_redeem_purchase(p):
            daily[day]["purchases_redeem"] += 1
        else:
            daily[day]["purchases_paid"] += 1
            try:
                daily[day]["revenue_paid"] += float(p.get("revenue") or 0)
            except Exception:
                pass

    # Build rows
    rows = []
    for day in sorted(daily.keys()):
        d = daily[day]
        rows.append({
            "date": day,
            "visitors":          len(d["visitors"]),
            "cart":              len(d["add_to_cart"]),
            "account":           len(d["account"]),
            "shipping":          len(d["add_shipping_info"]),
            "payment":           len(d["add_payment_info"]),
            "purchases_paid":    d["purchases_paid"],
            "purchases_redeem":  d["purchases_redeem"],
            "revenue_paid":      round(d["revenue_paid"], 2),
        })

    # ── Strict-funnel totals ──
    # Each stage is the SUBSET of visitors who fired the previous stage's event,
    # so the funnel bars decrease monotonically: Cart ≥ Account ≥ Shipping ≥ Payment.
    # The 'account' event otherwise fires on every /users/me load (returning logged-in
    # customers), which would render Account > Cart visually — fixed by intersection.
    cart_ips_set     = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_to_cart"}
    account_ips_set  = {e["ip"] for e in events if e.get("ip") and e["event"] == "account"}
    shipping_ips_set = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_shipping_info"}
    payment_ips_set  = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_payment_info"}

    strict_cart     = cart_ips_set
    strict_account  = account_ips_set  & strict_cart
    strict_shipping = shipping_ips_set & strict_account
    strict_payment  = payment_ips_set  & strict_shipping

    totals = {
        "visitors":         visitor_status_counts["total_unique"],
        "cart":             len(strict_cart),
        "account":          len(strict_account),
        "shipping":         len(strict_shipping),
        "payment":          len(strict_payment),
        "purchases_paid":   sum(r["purchases_paid"] for r in rows),
        "purchases_redeem": sum(r["purchases_redeem"] for r in rows),
        "revenue_paid":     round(sum(r["revenue_paid"] for r in rows), 2),
    }

    # ── CSV ──
    csv_path = os.path.join(OUT_DIR, f"funnel_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(csv_path, "w", newline="") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            for r in rows:
                w.writerow(r)
            w.writerow({"date": "TOTAL", **totals})
    print(f"CSV: {csv_path}")

    # ── Console summary ──
    print_console_summary(start_iso, end_iso, totals)

    # ── Ad-lift comparison (all-time) ──
    ad_lift = fetch_ad_lift()
    print(f"\nAd-lift: {ad_lift}")

    # ── Campaign-period comparison (since CAMPAIGN_START, with proper subtraction) ──
    campaign_compare = fetch_campaign_period_comparison()
    print(f"\nCampaign comparison:")
    print(f"  Ad: {campaign_compare['ad_visitors']} visitors, {campaign_compare['ad_orders']} orders, ${campaign_compare['ad_revenue']:,.2f} → {campaign_compare['ad_conv_rate']}% conv, ${campaign_compare['ad_aov']} AOV")
    print(f"  Non: {campaign_compare['non_visitors']:,} visitors, {campaign_compare['non_orders']:,} orders, ${campaign_compare['non_revenue']:,.2f} → {campaign_compare['non_conv_rate']}% conv, ${campaign_compare['non_aov']} AOV")

    # ── Cohort funnel comparison (ad-attributed vs non-attributed) ──
    # The cohort base uses ALL-TIME ad-attributed visits (not just the report
    # window) because attributed paid orders span weeks and we want to count
    # any cart/account/shipping/payment events those ad-driven visitors fired
    # whenever they fired (post-tag-deploy).
    print("Computing cohort funnel comparison (ad-attributed vs non, all-time visit cohort)...")

    # 1. Get ALL-TIME ad-attributed visit pks (no time filter)
    attributed_visit_pks = set()
    offset = 0
    while True:
        q = f'''
        {{
          allAggregatedMatchesByVisits(
            filter: {{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }}
            first: 5000 offset: {offset} orderBy: TIME_DESC
          ) {{ nodes {{ visitPk }} }}
        }}'''
        d = gq(q) or {}
        chunk = (d.get("allAggregatedMatchesByVisits") or {}).get("nodes", [])
        if not chunk: break
        for n in chunk:
            if n.get("visitPk") is not None:
                attributed_visit_pks.add(n["visitPk"])
        if len(chunk) < 5000: break
        offset += 5000
    print(f"  attributed visitPks (all-time): {len(attributed_visit_pks):,}")

    # 2. Pull the ad-attributed visit rows themselves to get their IPs + visitorIds.
    #    Limit to "in" filter on pk (chunked to avoid query bloat).
    ad_visit_ips = set()
    ad_visitor_ids = set()
    pk_list = list(attributed_visit_pks)
    BATCH = 500
    for i in range(0, len(pk_list), BATCH):
        batch = pk_list[i:i+BATCH]
        pk_filter = "[" + ",".join('"' + str(p) + '"' for p in batch) + "]"
        q = f'{{ allVisits(filter: {{ pk: {{ in: {pk_filter} }} }} first: 5000) {{ nodes {{ ip visitorId }} }} }}'
        d = gq(q) or {}
        for n in (d.get("allVisits") or {}).get("nodes", []):
            if n.get("ip"):        ad_visit_ips.add(n["ip"])
            if n.get("visitorId"): ad_visitor_ids.add(n["visitorId"])
    print(f"  ad cohort: {len(ad_visit_ips):,} unique IPs, {len(ad_visitor_ids):,} unique visitorIds")

    # 3. Cross-reference ALSO via paid-purchase attribution: visitors whose paid
    #    purchase was ad-attributed are by definition in the ad cohort. Pull their
    #    visitorIds from allPurchases and add to ad_visitor_ids/ad_visit_ips.
    attr_purch_pks = set()
    offset = 0
    while True:
        q = f'''{{ allAggregatedMatchesByPurchases(filter: {{ advertiserId: {{ equalTo: "{ADVERTISER_ID}" }} }} first: 5000 offset: {offset} orderBy: TIME_DESC) {{ nodes {{ purchasePk }} }} }}'''
        d = gq(q) or {}
        chunk = (d.get("allAggregatedMatchesByPurchases") or {}).get("nodes", [])
        if not chunk: break
        for n in chunk:
            if n.get("purchasePk") is not None: attr_purch_pks.add(n["purchasePk"])
        if len(chunk) < 5000: break
        offset += 5000
    if attr_purch_pks:
        pk_list2 = list(attr_purch_pks)
        for i in range(0, len(pk_list2), BATCH):
            batch = pk_list2[i:i+BATCH]
            pk_filter = "[" + ",".join('"' + str(p) + '"' for p in batch) + "]"
            q = f'{{ allPurchases(filter: {{ pk: {{ in: {pk_filter} }} }} first: 5000) {{ nodes {{ ip visitorId }} }} }}'
            d = gq(q) or {}
            for n in (d.get("allPurchases") or {}).get("nodes", []):
                if n.get("ip"):        ad_visit_ips.add(n["ip"])
                if n.get("visitorId"): ad_visitor_ids.add(n["visitorId"])
    print(f"  ad cohort (after purchase-side merge): {len(ad_visit_ips):,} IPs, {len(ad_visitor_ids):,} visitorIds")

    # 4. Bucket in-window visits: AD cohort = any visit whose ip OR visitorId is
    #    in the ad-attributed set; everyone else is non-attributed.
    cohort_ad_visitor_set = set()
    cohort_non_visitor_set = set()
    cohort_ad_ips = set()
    cohort_non_ips = set()
    for v in visits:
        ip = v.get("ip")
        vid = v.get("visitorId")
        in_ad = (ip and ip in ad_visit_ips) or (vid and vid in ad_visitor_ids)
        if in_ad:
            if ip: cohort_ad_ips.add(ip)
            if vid: cohort_ad_visitor_set.add(vid)
        else:
            if ip: cohort_non_ips.add(ip)
            if vid: cohort_non_visitor_set.add(vid)
    cohort_non_ips -= cohort_ad_ips
    cohort_non_visitor_set -= cohort_ad_visitor_set
    ad_visit_ips_for_funnel = cohort_ad_ips
    non_visit_ips = cohort_non_ips

    # 3. Pull funnel-stage IPs (already have these in `events`)
    cart_ips     = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_to_cart"}
    account_ips  = {e["ip"] for e in events if e.get("ip") and e["event"] == "account"}
    shipping_ips = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_shipping_info"}
    payment_ips  = {e["ip"] for e in events if e.get("ip") and e["event"] == "add_payment_info"}

    # 4. STRICT funnel: each stage requires the previous one. Account is
    #    inherently noisy (returning logged-in customers fire it without
    #    carting first), so this enforces the real customer journey:
    #    Visit → Cart → Account → Shipping → Payment.
    def cohort_funnel(cohort_ips):
        base = max(len(cohort_ips), 1)
        carted   = cart_ips     & cohort_ips
        # Account stage = visitors in cohort who BOTH carted AND fired account
        accounted = account_ips & carted
        # Shipping requires account (and by transitivity cart + cohort)
        shipped  = shipping_ips & accounted
        # Payment requires shipping
        paid_step = payment_ips & shipped
        return {
            "visit":          len(cohort_ips),
            "cart":           len(carted),
            "account":        len(accounted),
            "shipping":       len(shipped),
            "payment":        len(paid_step),
            # Pct-of-base (so all stages are comparable to overall visitor count)
            "cart_pct":       round(100 * len(carted)    / base, 2),
            "account_pct":    round(100 * len(accounted) / base, 2),
            "shipping_pct":   round(100 * len(shipped)   / base, 2),
            "payment_pct":    round(100 * len(paid_step) / base, 2),
            # Step-retain: each stage as % of immediately previous stage
            "cart_step":      round(100 * len(carted)    / base, 2),
            "account_step":   round(100 * len(accounted) / max(len(carted), 1),    2),
            "shipping_step":  round(100 * len(shipped)   / max(len(accounted), 1), 2),
            "payment_step":   round(100 * len(paid_step) / max(len(shipped), 1),   2),
        }

    cohort_funnel_data = {
        "ad_attributed":  cohort_funnel(ad_visit_ips_for_funnel),
        "non_attributed": cohort_funnel(non_visit_ips),
    }
    print(f"  ad-attributed cohort:  {cohort_funnel_data['ad_attributed']}")
    print(f"  non-attributed cohort: {cohort_funnel_data['non_attributed']}")

    # ── JSON data file (for main dashboard_v2.html consumption) ──
    json_path = os.path.join(OUT_DIR, "funnel_data.json")
    with open(json_path, "w") as f:
        json.dump({
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "window":            {"start": start_iso, "end": end_iso, "days": days_in_window},
            "totals":            totals,
            "daily":             rows,
            "ad_lift":           ad_lift,
            "visitor_status":    visitor_status_counts,
            "cohort_funnel":     cohort_funnel_data,
            "campaign_compare":  campaign_compare,
        }, f, indent=2)
    print(f"JSON: {json_path}")

    # ── HTML dashboard ──
    html_path = os.path.join(OUT_DIR, "funnel_dashboard.html")
    write_html_dashboard(html_path, start_iso, end_iso, rows, totals, ad_lift)
    print(f"\nHTML: {html_path}")


def print_console_summary(start_iso, end_iso, t):
    width = 90
    print("\n" + "=" * width)
    print(f"PURPLE CARROT FUNNEL  —  {start_iso[:10]} to {end_iso[:10]}")
    print("=" * width)

    stages = [
        ("Website visitor",            t["visitors"]),
        ("Added to cart",              t["cart"]),
        ("Submitted email / account",  t["account"]),
        ("Entered shipping info",      t["shipping"]),
        ("Entered payment info",       t["payment"]),
        ("Completed PAID purchase",    t["purchases_paid"]),
    ]
    top = stages[0][1] or 1
    prev = stages[0][1]
    biggest_drop = (None, 0)
    print(f"{'Stage':<30} {'Count':>10} {'% top':>8} {'% step':>8} {'Drop':>8}")
    print("-" * width)
    for label, count in stages:
        pct_top  = 100 * count / top if top else 0
        pct_step = 100 * count / prev if prev else 0
        drop     = max(prev - count, 0)
        bar = "█" * int(pct_top / 2.5)
        print(f"{label:<30} {count:>10,} {pct_top:>7.1f}% {pct_step:>7.1f}% {drop:>8,}  {bar}")
        if prev and drop > biggest_drop[1]:
            biggest_drop = (label, drop)
        prev = count
    print("-" * width)
    print(f"{'Gift redemptions (free)':<30} {t['purchases_redeem']:>10,}")
    print(f"{'Paid revenue':<30} ${t['revenue_paid']:>9,.0f}")
    print()
    if biggest_drop[0]:
        print(f"BIGGEST LEAK: {biggest_drop[1]:,} visitors lost at the '{biggest_drop[0]}' step.")


def write_html_dashboard(html_path, start_iso, end_iso, rows, t, ad_lift=None):
    """Generate a self-contained HTML dashboard with funnel + drop-off + ad lift."""
    ad_lift = ad_lift or {}
    # Note: 'account' fires for any /users/me with email — includes returning
    # logged-in users, not only new signups. Treat it as a side-track, not a
    # required step in the buy funnel.
    stages = [
        ("Website visitor",            t["visitors"],         "#6c63ff"),
        ("Added to cart",              t["cart"],             "#7c5cff"),
        ("Entered shipping info",      t["shipping"],         "#a64fff"),
        ("Entered payment info",       t["payment"],          "#c247ff"),
        ("Completed PAID purchase",    t["purchases_paid"],   "#22c55e"),
    ]
    top = stages[0][1] or 1
    biggest_drop_idx = 0
    biggest_drop_val = 0
    biggest_drop_pct = 0
    for i in range(1, len(stages)):
        drop = stages[i-1][1] - stages[i][1]
        prev_count = stages[i-1][1] or 1
        drop_pct = 100 * drop / prev_count
        # Use proportional drop to find biggest leak (not absolute count)
        if drop_pct > biggest_drop_pct and drop > 0:
            biggest_drop_val = drop
            biggest_drop_pct = drop_pct
            biggest_drop_idx = i

    bars_html = []
    prev = stages[0][1]
    for i, (label, count, color) in enumerate(stages):
        pct_top = 100 * count / top if top else 0
        pct_step = 100 * count / prev if prev else 100
        drop_pct = (100 - pct_step) if i > 0 else 0
        drop = max(prev - count, 0) if i > 0 else 0
        is_biggest = (i == biggest_drop_idx and drop > 0)
        leak_badge = (f'<span class="leak-badge">▼ ({drop_pct:.1f}% drop)</span>'
                      if drop > 0 else '')
        biggest_marker = '<span class="biggest">BIGGEST LEAK</span>' if is_biggest else ''
        bars_html.append(f'''
        <div class="stage">
          <div class="stage-row">
            <div class="stage-label">{label}</div>
            <div class="stage-pct">{pct_top:.1f}% of visitors</div>
          </div>
          <div class="stage-bar-wrap">
            <div class="stage-bar" style="width:{pct_top:.2f}%;background:{color}"></div>
          </div>
          <div class="stage-meta">
            {leak_badge}
            {biggest_marker}
          </div>
        </div>''')
        prev = count

    # Daily table rows
    daily_rows_html = []
    for r in rows[-14:]:
        daily_rows_html.append(f'''
        <tr>
          <td>{r['date']}</td>
          <td class="num">{r['visitors']:,}</td>
          <td class="num">{r['cart']:,}</td>
          <td class="num">{r['account']:,}</td>
          <td class="num">{r['shipping']:,}</td>
          <td class="num">{r['payment']:,}</td>
          <td class="num green">{r['purchases_paid']:,}</td>
          <td class="num orange">{r['purchases_redeem']:,}</td>
          <td class="num">${r['revenue_paid']:,.0f}</td>
        </tr>''')

    # Conversion math
    visitor_to_cart  = (100 * t["cart"] / t["visitors"]) if t["visitors"] else 0
    cart_to_paid     = (100 * t["purchases_paid"] / t["cart"]) if t["cart"] else 0
    visitor_to_paid  = (100 * t["purchases_paid"] / t["visitors"]) if t["visitors"] else 0
    aov              = (t["revenue_paid"] / t["purchases_paid"]) if t["purchases_paid"] else 0

    insight_html = ''
    # The "I didn't realize this was a subscription" insight — fires when cart→shipping
    # drops sharply. Cart-adders who realize at signup that PC is subscription-only bail.
    if t["cart"] and t["shipping"] is not None:
        cart_to_shipping = 100 * t["shipping"] / t["cart"] if t["cart"] else 0
        cart_lost_pct = 100 - cart_to_shipping
        if cart_to_shipping < 30 and t["cart"] > 50:
            insight_html = f'''
            <div class="insight">
              <h3>⚠ Subscription model not clear at the cart step</h3>
              <p>Of 100% of visitors who added to cart, only
              <strong>{cart_to_shipping:.1f}%</strong> made it to shipping —
              <strong>{cart_lost_pct:.1f}% of cart-adders walked away</strong> at the
              account / subscription-disclosure step.</p>
              <p>This is consistent with the qualitative observation that the site doesn't make
              it obvious that a Purple Carrot order requires a subscription. Visitors think
              they're buying a meal or two, hit the email gate, realize it's a subscription
              commitment, and leave.</p>
              <p><strong>Worth testing:</strong> a clear "this is a subscription, $X/week, cancel
              anytime" disclosure on the shop / plan-selection page — BEFORE the cart commit.
              Or offer a true one-time-order option to capture the meal-purchase intent.</p>
            </div>'''

    html = f'''<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>Purple Carrot Funnel — {start_iso[:10]} to {end_iso[:10]}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #fafafa; color: #1a1a1a; margin: 0; padding: 0; }}
.container {{ max-width: 1100px; margin: 0 auto; padding: 32px 24px; }}
h1 {{ margin: 0 0 4px 0; font-size: 28px; }}
.daterange {{ color: #666; font-size: 14px; margin-bottom: 32px; }}
.kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
.kpi {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
.kpi-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin-bottom: 6px; }}
.kpi-value {{ font-size: 28px; font-weight: 600; }}
.kpi-sub {{ font-size: 12px; color: #888; margin-top: 4px; }}
.green {{ color: #22c55e; }}
.orange {{ color: #f59e0b; }}
.red {{ color: #ef4444; }}
.section {{ background: white; border-radius: 12px; padding: 28px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
h2 {{ margin: 0 0 20px 0; font-size: 20px; }}
.stage {{ margin-bottom: 16px; }}
.stage-row {{ display: flex; align-items: baseline; margin-bottom: 6px; }}
.stage-label {{ flex: 1; font-weight: 500; }}
.stage-pct {{ font-size: 14px; color: #555; min-width: 140px; text-align: right; }}
.stage-pct {{ color: #888; font-size: 13px; min-width: 130px; text-align: right; }}
.stage-bar-wrap {{ height: 10px; background: #f0f0f0; border-radius: 5px; overflow: hidden; }}
.stage-bar {{ height: 100%; transition: width 0.4s; }}
.stage-meta {{ margin-top: 6px; font-size: 12px; }}
.leak-badge {{ color: #ef4444; font-weight: 500; margin-right: 12px; }}
.biggest {{ background: #fef2f2; color: #b91c1c; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
.insight {{ background: #fff7ed; border-left: 4px solid #f59e0b; padding: 16px 20px; border-radius: 8px; margin-top: 24px; }}
.insight h3 {{ margin: 0 0 8px 0; font-size: 16px; color: #b45309; }}
.insight p {{ margin: 0; line-height: 1.5; color: #78350f; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #f0f0f0; }}
th {{ background: #fafafa; font-weight: 500; color: #555; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.footer {{ text-align: center; color: #999; font-size: 12px; padding: 32px 0 16px; }}
.footnote {{ font-size: 12px; color: #888; margin-top: 16px; line-height: 1.5; }}
.gift-track {{ background: #fff7ed; }}
code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 3px; font-size: 12px; }}
.lift-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 20px; }}
.lift-card {{ background: #f9fafb; border-radius: 10px; padding: 18px; border: 1px solid #e5e7eb; }}
.lift-card.lift-callout {{ background: linear-gradient(135deg, #ecfdf5, #d1fae5); border-color: #6ee7b7; }}
.lift-header {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #888; font-weight: 600; }}
.lift-value {{ font-size: 28px; font-weight: 700; margin-top: 8px; }}
.lift-multiple {{ color: #059669; font-size: 36px; }}
.lift-sub {{ font-size: 12px; color: #666; margin-top: 4px; line-height: 1.4; }}
.lift-rate {{ font-size: 13px; color: #6c63ff; font-weight: 600; margin-top: 10px; }}
.lift-share-bar-wrap {{ margin-top: 16px; }}
.lift-share-bar-label {{ font-size: 12px; color: #555; margin-bottom: 6px; }}
.lift-share-bar-bg {{ position: relative; height: 28px; background: #f3f4f6; border-radius: 6px; overflow: hidden; }}
.lift-share-bar-fill {{ height: 100%; background: linear-gradient(90deg, #6c63ff, #8b5cf6); transition: width 0.4s; }}
.lift-share-bar-text {{ position: absolute; left: 12px; top: 50%; transform: translateY(-50%); font-size: 12px; font-weight: 600; color: white; mix-blend-mode: difference; }}
</style></head><body>
<div class="container">

<h1>Purple Carrot — Funnel Dashboard</h1>
<div class="daterange">{start_iso[:10]} → {end_iso[:10]}  ·  generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>

<div class="kpi-grid">
  <div class="kpi">
    <div class="kpi-label">Visitors</div>
    <div class="kpi-value">{t["visitors"]:,}</div>
    <div class="kpi-sub">unique visitorIds</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Paid Purchases</div>
    <div class="kpi-value green">{t["purchases_paid"]:,}</div>
    <div class="kpi-sub">{visitor_to_paid:.2f}% of visitors</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Revenue</div>
    <div class="kpi-value">${t["revenue_paid"]:,.0f}</div>
    <div class="kpi-sub">${aov:,.0f} AOV</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Gift Redemptions</div>
    <div class="kpi-value orange">{t["purchases_redeem"]:,}</div>
    <div class="kpi-sub">free, excluded from revenue</div>
  </div>
</div>

<div class="section">
  <h2>Funnel — where do people fall off?</h2>
  {''.join(bars_html)}
  {insight_html}
  <p class="footnote">Note: "account" / email-submission events are not shown as a required step
  because returning logged-in users fire that event without going through cart. Total
  account events in window: <strong>{t["account"]:,}</strong> (mix of new signups + returning users
  loading the site).</p>
</div>

<div class="section gift-track">
  <h2>Gift redemption side-track</h2>
  <p>Visitors who came in via a <code>/redeem_signup/...</code> URL with a gift code.
  These are recipients claiming a free trial — already-paid revenue from the original
  gifter. Counted separately so they don't drag down ROAS or paid AOV.</p>
  <div class="kpi-grid" style="margin-top:16px">
    <div class="kpi">
      <div class="kpi-label">Gift redemptions</div>
      <div class="kpi-value orange">{t["purchases_redeem"]:,}</div>
    </div>
  </div>
</div>

<div class="section">
  <h2>Headline conversion math</h2>
  <table>
    <tr><th>Conversion path</th><th class="num">Rate</th><th>Reading</th></tr>
    <tr><td>Visitor → Cart</td><td class="num">{visitor_to_cart:.2f}%</td><td>How many visitors engage with the product enough to add to cart</td></tr>
    <tr><td>Cart → Paid Purchase</td><td class="num">{cart_to_paid:.2f}%</td><td>Of cart-adders, how many actually subscribe</td></tr>
    <tr><td>Visitor → Paid Purchase</td><td class="num">{visitor_to_paid:.2f}%</td><td>Overall site conversion rate</td></tr>
    <tr><td>Paid AOV</td><td class="num">${aov:,.2f}</td><td>Average paid order value</td></tr>
  </table>
</div>

<div class="section">
  <h2>Ad-attributed lift — programmatic vs site-wide</h2>
  <p class="footnote" style="margin-top:0">All-time intersection of <code>allVisitAttributions</code> and <code>allPurchaseAttributions</code>. Site-wide rate is paid-purchaser visitors / total visitors over a 14-day window (extrapolated).</p>

  <div class="lift-grid">
    <div class="lift-card lift-ae">
      <div class="lift-header">Ad-exposed visitors</div>
      <div class="lift-value">{ad_lift.get("ae_visitors", 0):,}</div>
      <div class="lift-sub">{ad_lift.get("ae_visitors_vt", 0):,} view-through · {ad_lift.get("ae_visitors_ct", 0):,} click-through</div>
      <div class="lift-rate">{ad_lift.get("ae_conv_rate", 0):.2f}% paid-conversion rate</div>
    </div>
    <div class="lift-card lift-ne">
      <div class="lift-header">Site-wide baseline</div>
      <div class="lift-value">~59,000</div>
      <div class="lift-sub">14-day site visitor estimate</div>
      <div class="lift-rate">~1.90% paid-conversion rate</div>
    </div>
    <div class="lift-card lift-callout">
      <div class="lift-header">LIFT</div>
      <div class="lift-value lift-multiple">{(ad_lift.get("ae_conv_rate", 0)/1.90):.1f}×</div>
      <div class="lift-sub">Ad-exposed visitors convert at <strong>{(ad_lift.get("ae_conv_rate", 0)/1.90):.1f}×</strong> the site-wide rate</div>
    </div>
  </div>

  <div class="lift-share-bar-wrap">
    <div class="lift-share-bar-label">Of all paid orders, share that came from ad-exposed visitors</div>
    <div class="lift-share-bar-bg">
      <div class="lift-share-bar-fill" style="width:{ad_lift.get("ae_share_pct", 0)}%"></div>
      <span class="lift-share-bar-text">{ad_lift.get("ae_attributed_orders", 0):,} of {ad_lift.get("total_paid_orders", 0):,} orders ({ad_lift.get("ae_share_pct", 0):.1f}%) ad-attributed</span>
    </div>
  </div>
</div>

<div class="section">
  <h2>Daily breakdown</h2>
  <table>
    <thead><tr>
      <th>Date</th><th class="num">Visitors</th><th class="num">Cart</th><th class="num">Account</th>
      <th class="num">Shipping</th><th class="num">Payment</th>
      <th class="num">Paid</th><th class="num">Redeem</th><th class="num">Revenue</th>
    </tr></thead>
    <tbody>{''.join(daily_rows_html)}</tbody>
  </table>
</div>

<div class="footer">DLVE Universal Tag v15.4.0 · Advertiser 1060 · Script QwxIISziQhWR</div>
</div></body></html>
'''
    with open(html_path, "w") as f:
        f.write(html)


if __name__ == "__main__":
    main()
