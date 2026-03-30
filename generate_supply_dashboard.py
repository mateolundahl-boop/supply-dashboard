#!/usr/bin/env python3
"""
Supply CRM Campaign Dashboard Generator — v2
==============================================
Queries Redshift and generates a fully interactive HTML dashboard with Kavak branding.

Key changes from v1:
  - Separate WA vs Email channels everywhere
  - Interactive filters: Channel (WA/Email/Both), Vertical (Supply/With Spillover), Time (Week/Month/Quarter)
  - All filtering/aggregation done client-side in JS
  - Temporal funnel evolution (replaces static funnel)
  - OS/1K Deliveries as central efficiency metric
  - CPOS respects vertical filter and channel costs
  - Click-to-expand modals on charts
  - No "OS by Medium" section
  - Proper Y-axis formatting (K/M)

Usage: python generate_supply_dashboard.py
"""

import sys
import json
import os
from datetime import datetime, timedelta
import webbrowser

import pandas as pd
import numpy as np
from query_runner import execute_query

# ─── Configuration ───────────────────────────────────────────────────────────
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
NUM_WEEKS = 26
WA_COST_MXN = 0.40
EMAIL_COST_MXN = 0.02
SATURATION_WEEKS = 12  # Pulse/saturation queries use fewer weeks (heavy joins)

# ─── Campaign Classification CASE Fragments ─────────────────────────────────
# NOTE: Use %% for LIKE wildcards because these are inside .format() strings

CAMPAIGN_TYPE_CASE = """
CASE
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%emailjourney%%' THEN 'Email Cross'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%triggered%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%regnoos%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%osnomade%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%quotenoreg%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%offererror%%' THEN 'Triggered'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%dormreac%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%dormant%%' THEN 'Dormant Reac'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%customer%%' THEN 'Customers'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%onboarding%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%welcome%%' THEN 'Onboarding'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%priceanch%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%pulse%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%weeklyoffer%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%wo[_]%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%tradein%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajati%%' THEN 'Pulse'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%sorteo%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%concurso%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%promo%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%bono%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%inctrue%%' THEN 'Promos'
    ELSE 'Others'
END
"""

DETAIL_CASE = """
CASE
    -- Email Cross subtypes
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%emailjourney%%'
      AND LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%mystery%%' THEN 'EC: Mystery Offer'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%emailjourney%%' THEN 'EC: Gen'
    -- Triggered subtypes
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%regnoos%%' THEN 'Trig: Reg No OS'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%osnomade%%' THEN 'Trig: OS No Made'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%quotenoreg%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%quotnoreg%%' THEN 'Trig: Quot No Reg'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%offererror%%' THEN 'Trig: Offer Error'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%triggered%%' THEN 'Trig: Other'
    -- Dormant
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%dormreac%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%dormant%%' THEN 'Dormant Reac'
    -- Customers
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%customer%%' THEN 'Customers'
    -- Onboarding subtypes
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%onboarding%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%welcome%%' THEN 'Onboarding'
    -- Pulse subtypes
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%tradein%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajati%%' THEN 'Pulse: Trade In'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajawow%%'
      AND LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) NOT LIKE '%%cajawowmd%%' THEN 'Pulse: WoW >5%%'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajawowmd%%' THEN 'Pulse: WoW+MD'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajamd%%' THEN 'Pulse: MD <15%%'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajaofferinst%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%offerinst%%' THEN 'Pulse: Instant Offer'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%cajanofferinst%%' THEN 'Pulse: No Instant Offer'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%weeklyoffer%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%wo[_]%%' THEN 'Pulse: Weekly Offer'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%priceanch%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%pulse%%' THEN 'Pulse: PriceAnch'
    -- Promos subtypes
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%sorteo%%' THEN 'Promos: Sorteo'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%concurso%%' THEN 'Promos: Concurso'
    WHEN LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%promo%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%bono%%'
      OR LOWER(COALESCE(NULLIF(TRIM({col}), ''), '')) LIKE '%%inctrue%%' THEN 'Promos: Other'
    ELSE 'Others'
END
"""

# ─── Query Definitions ───────────────────────────────────────────────────────

QUERIES = {}

# Q1: engagement_all — Weekly engagement by channel (WA + Email separately)
QUERIES['engagement_all'] = f"""
SELECT
    DATE_TRUNC('week', campaign_delivery_time)::DATE AS week,
    channel,
    SUM(CASE WHEN delivered = TRUE THEN 1 ELSE 0 END) AS deliveries,
    COUNT(DISTINCT CASE WHEN delivered = TRUE THEN cdp_customer_id END) AS unique_users,
    SUM(CASE WHEN opened = TRUE THEN 1 ELSE 0 END) AS opens,
    SUM(CASE WHEN clicked = TRUE THEN 1 ELSE 0 END) AS clicks
FROM cdp_global_serving.crm_campaigns
WHERE campaign_delivery_time >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{NUM_WEEKS} weeks'
  AND campaign_delivery_time < DATE_TRUNC('week', CURRENT_DATE)
  AND channel IN ('whatsapp', 'email')
  AND country_iso = 'MX'
  AND LOWER(name) LIKE '%%purchase%%'
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Q2: engagement_by_type — Weekly engagement by channel x campaign_type
QUERIES['engagement_by_type'] = """
SELECT
    DATE_TRUNC('week', campaign_delivery_time)::DATE AS week,
    channel,
    {case} AS campaign_type,
    SUM(CASE WHEN delivered = TRUE THEN 1 ELSE 0 END) AS deliveries,
    SUM(CASE WHEN opened = TRUE THEN 1 ELSE 0 END) AS opens,
    SUM(CASE WHEN clicked = TRUE THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN delivered = TRUE AND LOWER(name) LIKE '%%inctrue%%' THEN 1 ELSE 0 END) AS promo_deliveries
FROM cdp_global_serving.crm_campaigns
WHERE campaign_delivery_time >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{n} weeks'
  AND campaign_delivery_time < DATE_TRUNC('week', CURRENT_DATE)
  AND channel IN ('whatsapp', 'email')
  AND country_iso = 'MX'
  AND LOWER(name) LIKE '%%purchase%%'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""".format(case=CAMPAIGN_TYPE_CASE.format(col='name'), n=NUM_WEEKS)

# Q3: engagement_detail — Weekly engagement by channel x campaign_detail
QUERIES['engagement_detail'] = """
SELECT
    DATE_TRUNC('week', campaign_delivery_time)::DATE AS week,
    channel,
    {case} AS campaign_detail,
    SUM(CASE WHEN delivered = TRUE THEN 1 ELSE 0 END) AS deliveries,
    SUM(CASE WHEN opened = TRUE THEN 1 ELSE 0 END) AS opens,
    SUM(CASE WHEN clicked = TRUE THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN delivered = TRUE AND LOWER(name) LIKE '%%inctrue%%' THEN 1 ELSE 0 END) AS promo_deliveries
FROM cdp_global_serving.crm_campaigns
WHERE campaign_delivery_time >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{n} weeks'
  AND campaign_delivery_time < DATE_TRUNC('week', CURRENT_DATE)
  AND channel IN ('whatsapp', 'email')
  AND country_iso = 'MX'
  AND LOWER(name) LIKE '%%purchase%%'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""".format(case=DETAIL_CASE.format(col='name'), n=NUM_WEEKS)

# Q4: os_weekly — Weekly OS by vertical + channel
# Vertical defined by campaign_helper keywords (matching user's reference query)
# Channel mapped from medium groups (matching user's reference)
# Uses paid_non_paid = 'CRM' (not source = 'insider') to match user's reference
_VERTICAL_CASE = """
CASE
    WHEN CHARINDEX('purchase', campaign_helper) > 0 THEN 'Supply'
    WHEN CHARINDEX('sale', campaign_helper) > 0 THEN 'Sales'
    WHEN CHARINDEX('kuna', campaign_helper) > 0 THEN 'Auto Equity'
    ELSE 'Other'
END
"""
_CHANNEL_CASE = """
CASE
    WHEN medium IN ('architect_whatsapp', 'whatsapp_business_api', 'whatsapp-business-api') THEN 'whatsapp'
    WHEN medium IN ('architect_email', 'emailrecommendation') THEN 'email'
    ELSE 'other'
END
"""

QUERIES['os_weekly'] = f"""
SELECT
    DATE_TRUNC('week', date)::DATE AS week,
    {_VERTICAL_CASE} AS vertical,
    {_CHANNEL_CASE} AS channel,
    SUM(tgt_attribution_inspections) AS os
FROM serving.attribution_l2_target_date_rs
WHERE date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{NUM_WEEKS} weeks'
  AND date < DATE_TRUNC('week', CURRENT_DATE)
  AND paid_non_paid = 'CRM'
  AND country = 'MX'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Q5: os_by_type — Weekly OS by vertical x campaign_type_label x channel
_l2_col = "COALESCE(NULLIF(TRIM(campaign_name), ''), campaign_helper)"
QUERIES['os_by_type'] = """
SELECT
    DATE_TRUNC('week', date)::DATE AS week,
    {vert} AS vertical,
    {ch} AS channel,
    {case} AS campaign_type_label,
    SUM(tgt_attribution_inspections) AS os
FROM serving.attribution_l2_target_date_rs
WHERE date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{n} weeks'
  AND date < DATE_TRUNC('week', CURRENT_DATE)
  AND paid_non_paid = 'CRM'
  AND country = 'MX'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
""".format(case=CAMPAIGN_TYPE_CASE.format(col=_l2_col), vert=_VERTICAL_CASE, ch=_CHANNEL_CASE, n=NUM_WEEKS)

# Q6: os_detail — Weekly OS by vertical x campaign_detail x channel
QUERIES['os_detail'] = """
SELECT
    DATE_TRUNC('week', date)::DATE AS week,
    {vert} AS vertical,
    {ch} AS channel,
    {case} AS campaign_detail,
    SUM(tgt_attribution_inspections) AS os
FROM serving.attribution_l2_target_date_rs
WHERE date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{n} weeks'
  AND date < DATE_TRUNC('week', CURRENT_DATE)
  AND paid_non_paid = 'CRM'
  AND country = 'MX'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
""".format(case=DETAIL_CASE.format(col=_l2_col), vert=_VERTICAL_CASE, ch=_CHANNEL_CASE, n=NUM_WEEKS)

# Q7: md_weekly — MD weekly from register_velocity
QUERIES['md_weekly'] = f"""
SELECT
    DATE_TRUNC('week', register_creation_date)::DATE AS week,
    ROUND(AVG(1.0 - COALESCE(instant_offer, tradein_offer) / NULLIF(market_price, 0)) * 100, 2) AS avg_md_pct,
    COUNT(*) AS registers
FROM serving.register_velocity
WHERE register_creation_date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{NUM_WEEKS} weeks'
  AND register_creation_date < DATE_TRUNC('week', CURRENT_DATE)
  AND country = 'MX'
  AND COALESCE(flag_kuna, 'F') = 'F'
  AND COALESCE(purchase_order_is_financing_total_loss, false) = false
  AND market_price IS NOT NULL
  AND market_price > 0
GROUP BY 1
ORDER BY 1
"""

# Q8: saturation_frequency — WA frequency distribution for Pulse audience per week
# With pulse_type dimension for client-side filtering
# Optimized: pre-filter campaigns with date range and use INNER JOIN in wa_sends
QUERIES['saturation_frequency'] = f"""
WITH date_bounds AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{SATURATION_WEEKS} weeks' AS start_dt,
        DATE_TRUNC('week', CURRENT_DATE) AS end_dt
),
pulse_users AS (
    SELECT DISTINCT
        DATE_TRUNC('week', p.pulse_date)::DATE AS pulse_week,
        p.pulse_type,
        c.cdp_customer_id
    FROM playground.pulse_details p
    JOIN cdp_global_serving.customer c ON p.olimpo_id::VARCHAR = c.legacy_id::VARCHAR
    CROSS JOIN date_bounds d
    WHERE p.country_code = 'MX'
      AND p.pulse_date >= d.start_dt
      AND p.pulse_date < d.end_dt
),
wa_sends AS (
    SELECT
        DATE_TRUNC('week', cam.campaign_delivery_time)::DATE AS send_week,
        pu.pulse_type,
        cam.cdp_customer_id,
        COUNT(*) AS wa_count
    FROM cdp_global_serving.crm_campaigns cam
    JOIN pulse_users pu
        ON cam.cdp_customer_id = pu.cdp_customer_id
        AND DATE_TRUNC('week', cam.campaign_delivery_time)::DATE = pu.pulse_week
    CROSS JOIN date_bounds d
    WHERE cam.channel = 'whatsapp'
      AND cam.delivered = TRUE
      AND cam.campaign_delivery_time >= d.start_dt
      AND cam.campaign_delivery_time < d.end_dt
    GROUP BY 1, 2, 3
)
SELECT
    pu.pulse_week AS week,
    pu.pulse_type,
    COUNT(DISTINCT pu.cdp_customer_id) AS total_pulse_users,
    COUNT(DISTINCT ws.cdp_customer_id) AS users_with_wa,
    COALESCE(SUM(ws.wa_count), 0) AS total_wa_sends,
    COUNT(DISTINCT CASE WHEN ws.wa_count = 1 THEN ws.cdp_customer_id END) AS freq_1,
    COUNT(DISTINCT CASE WHEN ws.wa_count = 2 THEN ws.cdp_customer_id END) AS freq_2,
    COUNT(DISTINCT CASE WHEN ws.wa_count = 3 THEN ws.cdp_customer_id END) AS freq_3,
    COUNT(DISTINCT CASE WHEN ws.wa_count >= 4 THEN ws.cdp_customer_id END) AS freq_4plus
FROM pulse_users pu
LEFT JOIN wa_sends ws
    ON pu.cdp_customer_id = ws.cdp_customer_id
    AND pu.pulse_week = ws.send_week
    AND pu.pulse_type = ws.pulse_type
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Q9: vertical_overlap — Users receiving WA from Supply vs AE(Kuna) vs Sales by week
# Mutually exclusive categories (no double-counting). With pulse_type dimension.
QUERIES['vertical_overlap'] = f"""
WITH pulse_wa AS (
    SELECT
        DATE_TRUNC('week', p.pulse_date)::DATE AS week,
        p.pulse_type,
        c.cdp_customer_id,
        MAX(CASE WHEN LOWER(cam.name) LIKE '%%purchase%%' THEN 1 ELSE 0 END) AS got_supply,
        MAX(CASE WHEN LOWER(cam.name) LIKE '%%kuna%%' THEN 1 ELSE 0 END) AS got_ae,
        MAX(CASE WHEN LOWER(cam.name) LIKE '%%sale%%'
                  AND LOWER(cam.name) NOT LIKE '%%purchase%%'
                  AND LOWER(cam.name) NOT LIKE '%%kuna%%' THEN 1 ELSE 0 END) AS got_sales
    FROM playground.pulse_details p
    JOIN cdp_global_serving.customer c
        ON p.olimpo_id::VARCHAR = c.legacy_id::VARCHAR
    JOIN cdp_global_serving.crm_campaigns cam
        ON c.cdp_customer_id = cam.cdp_customer_id
        AND cam.channel = 'whatsapp'
        AND cam.delivered = TRUE
        AND cam.campaign_delivery_time >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{SATURATION_WEEKS} weeks'
        AND cam.campaign_delivery_time < DATE_TRUNC('week', CURRENT_DATE)
        AND DATE_TRUNC('week', cam.campaign_delivery_time)::DATE = DATE_TRUNC('week', p.pulse_date)::DATE
    WHERE p.country_code = 'MX'
      AND p.pulse_date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{SATURATION_WEEKS} weeks'
      AND p.pulse_date < DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY 1, 2, 3
)
SELECT
    week,
    pulse_type,
    COUNT(*) AS users_with_any_wa,
    SUM(CASE WHEN got_supply = 1 AND got_ae = 0 AND got_sales = 0 THEN 1 ELSE 0 END) AS supply_only,
    SUM(CASE WHEN got_ae = 1 AND got_supply = 0 AND got_sales = 0 THEN 1 ELSE 0 END) AS ae_only,
    SUM(CASE WHEN got_sales = 1 AND got_supply = 0 AND got_ae = 0 THEN 1 ELSE 0 END) AS sales_only,
    SUM(CASE WHEN got_supply = 1 AND got_ae = 1 AND got_sales = 0 THEN 1 ELSE 0 END) AS supply_ae_only,
    SUM(CASE WHEN got_supply = 1 AND got_sales = 1 AND got_ae = 0 THEN 1 ELSE 0 END) AS supply_sales_only,
    SUM(CASE WHEN got_ae = 1 AND got_sales = 1 AND got_supply = 0 THEN 1 ELSE 0 END) AS ae_sales_only,
    SUM(CASE WHEN got_supply = 1 AND got_ae = 1 AND got_sales = 1 THEN 1 ELSE 0 END) AS all_three,
    SUM(CASE WHEN (got_supply + got_ae + got_sales) >= 2 THEN 1 ELSE 0 END) AS multi_vertical_users
FROM pulse_wa
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Q10: value_prop_repetition — VP mix and repetition rate for Pulse users
# VP classification by caja keywords (not DETAIL_CASE):
#   cajawowmd → WoW+MD, cajawow (not md) → WoW, cajamd → MD,
#   offerinst/cajaofferinst → Instant Offer, tradein/cajati → Trade In, else → Generico
QUERIES['value_prop_repetition'] = f"""
WITH pulse_users AS (
    SELECT DISTINCT
        DATE_TRUNC('week', p.pulse_date)::DATE AS pulse_week,
        p.pulse_type,
        c.cdp_customer_id
    FROM playground.pulse_details p
    JOIN cdp_global_serving.customer c ON p.olimpo_id::VARCHAR = c.legacy_id::VARCHAR
    WHERE p.country_code = 'MX'
      AND p.pulse_date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{SATURATION_WEEKS} weeks'
      AND p.pulse_date < DATE_TRUNC('week', CURRENT_DATE)
),
user_campaign_vps AS (
    SELECT
        pu.pulse_week,
        pu.pulse_type,
        pu.cdp_customer_id,
        CASE
            WHEN LOWER(cam.name) LIKE '%cajawowmd%' THEN 'WoW+MD'
            WHEN LOWER(cam.name) LIKE '%cajawow%' THEN 'WoW'
            WHEN LOWER(cam.name) LIKE '%cajamd%' THEN 'MD'
            WHEN LOWER(cam.name) LIKE '%cajaofferinst%'
              OR LOWER(cam.name) LIKE '%offerinst%' THEN 'Instant Offer'
            WHEN LOWER(cam.name) LIKE '%tradein%'
              OR LOWER(cam.name) LIKE '%cajati%' THEN 'Trade In'
            ELSE 'Generico'
        END AS vp_label,
        COUNT(*) AS sends
    FROM pulse_users pu
    JOIN cdp_global_serving.crm_campaigns cam
        ON pu.cdp_customer_id = cam.cdp_customer_id
        AND cam.channel = 'whatsapp'
        AND cam.delivered = TRUE
        AND DATE_TRUNC('week', cam.campaign_delivery_time)::DATE = pu.pulse_week
        AND cam.campaign_delivery_time >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '{SATURATION_WEEKS} weeks'
        AND cam.campaign_delivery_time < DATE_TRUNC('week', CURRENT_DATE)
    WHERE LOWER(cam.name) LIKE '%purchase%'
    GROUP BY 1, 2, 3, 4
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cdp_customer_id, pulse_week ORDER BY sends DESC) AS rn
    FROM user_campaign_vps
),
primary_vp AS (
    SELECT pulse_week, pulse_type, cdp_customer_id, vp_label AS primary_vp
    FROM ranked WHERE rn = 1
),
with_lag AS (
    SELECT *,
        LAG(primary_vp) OVER (PARTITION BY cdp_customer_id ORDER BY pulse_week) AS prev_vp
    FROM primary_vp
)
SELECT
    pulse_week AS week,
    pulse_type,
    COUNT(DISTINCT cdp_customer_id) AS users_with_wa,
    COUNT(DISTINCT CASE WHEN primary_vp = prev_vp THEN cdp_customer_id END) AS repeated_vp_users,
    COUNT(DISTINCT CASE WHEN primary_vp = 'WoW' THEN cdp_customer_id END) AS vp_wow,
    COUNT(DISTINCT CASE WHEN primary_vp = 'WoW+MD' THEN cdp_customer_id END) AS vp_wowmd,
    COUNT(DISTINCT CASE WHEN primary_vp = 'MD' THEN cdp_customer_id END) AS vp_md,
    COUNT(DISTINCT CASE WHEN primary_vp = 'Instant Offer' THEN cdp_customer_id END) AS vp_offer,
    COUNT(DISTINCT CASE WHEN primary_vp = 'Trade In' THEN cdp_customer_id END) AS vp_tradein,
    COUNT(DISTINCT CASE WHEN primary_vp = 'Generico' THEN cdp_customer_id END) AS vp_generico
FROM with_lag
GROUP BY 1, 2
ORDER BY 1, 2
"""


# ─── Data Fetching ───────────────────────────────────────────────────────────

def fetch_all_data():
    """Execute all queries and return results as dict of DataFrames.
    Retries failed queries up to 2 times with a 10s pause between attempts.
    """
    import time as _time
    MAX_RETRIES = 2
    results = {}
    for name, query in QUERIES.items():
        last_err = None
        for attempt in range(1 + MAX_RETRIES):
            try:
                prefix = f"  Running {name}..." if attempt == 0 else f"  Retrying {name} (attempt {attempt + 1})..."
                print(prefix, flush=True)
                df = execute_query(query)
                results[name] = df
                print(f"  OK {name}: {len(df)} rows", flush=True)
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < MAX_RETRIES:
                    print(f"  FAIL {name}: {e} — retrying in 10s...", flush=True)
                    _time.sleep(10)
                else:
                    print(f"  FAIL {name} (gave up after {MAX_RETRIES + 1} attempts): {e}", flush=True)
        if last_err is not None:
            results[name] = pd.DataFrame()
    return results


# ─── Helpers ─────────────────────────────────────────────────────────────────

def safe_int(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0
    return int(val)


def safe_float(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    return float(val)


def df_to_records(df):
    """Convert DataFrame to list of dicts, handling NaN, dates, and Decimal/str numerics."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        else:
            # Try to convert string/Decimal columns to numeric (Redshift ROUND returns strings)
            try:
                converted = pd.to_numeric(df[col], errors='coerce')
                if converted.notna().any() and col != 'week':
                    df[col] = converted.fillna(0)
            except (ValueError, TypeError):
                pass
    return df.to_dict('records')


def detect_wtd(weeks_list):
    """Always return False — queries now exclude the current incomplete week."""
    return False


# ─── Prepare Data for Embedding ──────────────────────────────────────────────

def prepare_raw_data(raw):
    """Convert all DataFrames to JSON-serializable dicts and detect WTD."""
    data = {}

    # Convert each query result to records
    for key in ['engagement_all', 'engagement_by_type', 'engagement_detail',
                'os_weekly', 'os_by_type', 'os_detail', 'md_weekly',
                'saturation_frequency', 'vertical_overlap', 'value_prop_repetition']:
        df = raw.get(key, pd.DataFrame())
        if not df.empty and 'week' in df.columns:
            df['week'] = pd.to_datetime(df['week'])
        data[key] = df_to_records(df)

    # Detect WTD from engagement_all
    all_weeks = set()
    for rec in data.get('engagement_all', []):
        all_weeks.add(rec.get('week', ''))
    data['has_wtd'] = detect_wtd(all_weeks)

    data['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['wa_cost'] = WA_COST_MXN
    data['email_cost'] = EMAIL_COST_MXN

    return data


# ─── HTML Generation ─────────────────────────────────────────────────────────

def generate_html(data):
    """Generate the complete interactive dashboard HTML."""
    data_json = json.dumps(data, default=str)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=1500,initial-scale=1">
<title>Supply CRM Dashboard v2 — Kavak Lifecycle MX</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
    --kavak-blue: #0467FC;
    --kavak-dark: #1B2532;
    --kavak-darker: #131c27;
    --kavak-green: #00C48C;
    --kavak-red: #FF4757;
    --kavak-orange: #FF9F43;
    --kavak-purple: #A855F7;
    --kavak-text: rgba(255,255,255,0.92);
    --kavak-text-sec: rgba(255,255,255,0.5);
    --kavak-card: rgba(255,255,255,0.04);
    --kavak-border: rgba(255,255,255,0.08);
    --wa-color: #25D366;
    --email-color: #FF9F43;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    font-family: 'Inter', -apple-system, sans-serif;
    background: var(--kavak-darker);
    color: var(--kavak-text);
    min-height: 100vh;
}}

/* ── Sticky Header ── */
.header {{
    background: var(--kavak-dark);
    border-bottom: 1px solid var(--kavak-border);
    padding: 12px 32px;
    position: sticky;
    top: 0;
    z-index: 100;
}}
.header-top {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;
}}
.header-left {{ display: flex; align-items: center; gap: 16px; }}
.header-logo svg {{ width: 80px; height: auto; fill: var(--kavak-blue); }}
.header-title {{ font-size: 16px; font-weight: 700; }}
.header-subtitle {{ font-size: 11px; color: var(--kavak-text-sec); margin-top: 2px; }}
.header-meta {{ text-align: right; font-size: 11px; color: var(--kavak-text-sec); }}

/* ── Filter Bar ── */
.filter-bar {{
    display: flex;
    align-items: center;
    gap: 24px;
    flex-wrap: wrap;
}}
.filter-group {{
    display: flex;
    align-items: center;
    gap: 6px;
}}
.filter-label {{
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--kavak-text-sec);
    margin-right: 4px;
}}
.pill {{
    padding: 5px 14px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--kavak-border);
    background: transparent;
    color: var(--kavak-text-sec);
    transition: all 0.15s;
}}
.pill:hover {{ border-color: var(--kavak-blue); color: var(--kavak-text); }}
.pill.active {{
    background: var(--kavak-blue);
    border-color: var(--kavak-blue);
    color: #fff;
}}
/* ── Period Select Dropdown ── */
.period-select {{
    padding: 4px 10px;
    border-radius: 8px;
    font-size: 11px;
    font-weight: 600;
    border: 1px solid var(--kavak-border);
    background: var(--kavak-bg);
    color: var(--kavak-text);
    cursor: pointer;
    outline: none;
    min-width: 90px;
}}
.period-select:hover {{ border-color: var(--kavak-blue); }}
.period-select:focus {{ border-color: var(--kavak-blue); box-shadow: 0 0 0 2px rgba(4,103,252,0.2); }}
.pill.active-wa {{
    background: var(--wa-color);
    border-color: var(--wa-color);
    color: #000;
}}
.pill.active-email {{
    background: var(--email-color);
    border-color: var(--email-color);
    color: #000;
}}

/* ── Tab Navigation ── */
.tab-bar {{
    display: flex;
    gap: 0;
    margin-bottom: 0;
    border-bottom: 2px solid var(--kavak-border);
}}
.tab-btn {{
    padding: 10px 24px;
    font-family: 'Inter', sans-serif;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--kavak-text-sec);
    background: transparent;
    border: none;
    cursor: pointer;
    border-bottom: 3px solid transparent;
    margin-bottom: -2px;
    transition: all 0.15s;
}}
.tab-btn:hover {{ color: var(--kavak-text); }}
.tab-btn.active {{
    color: var(--kavak-blue);
    border-bottom-color: var(--kavak-blue);
}}
.tab-btn .tab-icon {{ margin-right: 6px; }}
.tab-content {{ display: none; }}
.tab-content.active {{ display: block; }}

/* ── Main ── */
.main {{ padding: 24px 32px; max-width: 1560px; margin: 0 auto; }}
.section {{ margin-bottom: 28px; }}
.section-title {{
    font-size: 13px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--kavak-text-sec);
    margin-bottom: 14px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--kavak-border);
    display: flex;
    align-items: center;
    gap: 8px;
}}
.channel-tag {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.channel-tag.wa {{ background: rgba(37,211,102,0.15); color: var(--wa-color); }}
.channel-tag.email {{ background: rgba(255,159,67,0.15); color: var(--email-color); }}
.channel-tag.both {{ background: rgba(4,103,252,0.15); color: var(--kavak-blue); }}

/* ── KPI Cards ── */
.kpi-grid {{
    display: grid;
    grid-template-columns: repeat(7, 1fr);
    gap: 10px;
}}
.kpi-card {{
    background: var(--kavak-card);
    border: 1px solid var(--kavak-border);
    border-radius: 10px;
    padding: 14px;
    position: relative;
    overflow: hidden;
}}
.kpi-label {{ font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; color: var(--kavak-text-sec); margin-bottom: 4px; display: flex; align-items: center; gap: 6px; }}
.kpi-value {{ font-size: 26px; font-weight: 800; line-height: 1.1; }}
.kpi-meta {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 6px; font-size: 10px; color: var(--kavak-text-sec); }}
.kpi-badge {{ font-weight: 700; }}
.kpi-badge.positive {{ color: var(--kavak-green); }}
.kpi-badge.negative {{ color: var(--kavak-red); }}
.kpi-sparkline {{ margin-top: 8px; height: 28px; }}
.kpi-sparkline canvas {{ width: 100% !important; height: 28px !important; }}
.kpi-wtd {{ font-size: 10px; color: var(--kavak-text-sec); margin-top: 4px; }}

/* ── Charts ── */
.charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
.chart-box {{
    background: var(--kavak-card);
    border: 1px solid var(--kavak-border);
    border-radius: 10px;
    padding: 18px;
    cursor: pointer;
    transition: border-color 0.2s;
}}
.chart-box:hover {{ border-color: var(--kavak-blue); }}
.chart-box h3 {{
    font-size: 12px;
    font-weight: 600;
    color: var(--kavak-text-sec);
    margin-bottom: 10px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    display: flex;
    align-items: center;
    gap: 8px;
}}
.chart-box h3 .expand-hint {{
    font-size: 9px;
    color: var(--kavak-text-sec);
    opacity: 0.5;
    margin-left: auto;
    font-weight: 400;
    text-transform: none;
    letter-spacing: 0;
}}
.chart-container {{ position: relative; height: 260px; }}
.chart-full {{ height: 100%; }}

/* ── Modal ── */
.modal-overlay {{
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.75);
    z-index: 1000;
    align-items: center;
    justify-content: center;
}}
.modal-overlay.show {{ display: flex; }}
.modal-content {{
    background: var(--kavak-dark);
    border: 1px solid var(--kavak-border);
    border-radius: 14px;
    padding: 24px;
    width: 90vw;
    max-width: 1100px;
    max-height: 80vh;
    position: relative;
}}
.modal-close {{
    position: absolute;
    top: 12px;
    right: 16px;
    background: none;
    border: none;
    color: var(--kavak-text-sec);
    font-size: 20px;
    cursor: pointer;
}}
.modal-close:hover {{ color: var(--kavak-text); }}
.modal-chart {{ height: 55vh; position: relative; }}
.legend-hint {{
    text-align: center;
    color: var(--kavak-text-sec);
    font-size: 11px;
    margin-top: 10px;
    opacity: 0.7;
}}

/* ── Temporal Funnel ── */
.funnel-chart-container {{
    background: var(--kavak-card);
    border: 1px solid var(--kavak-border);
    border-radius: 10px;
    padding: 18px;
    cursor: pointer;
    transition: border-color 0.2s;
}}
.funnel-chart-container:hover {{ border-color: var(--kavak-blue); }}
.funnel-chart-container h3 {{
    font-size: 12px; font-weight: 600; color: var(--kavak-text-sec);
    margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.8px;
    display: flex; align-items: center; gap: 8px;
}}
.funnel-chart-wrap {{ position: relative; height: 280px; }}

/* ── Tables ── */
.table-wrap {{
    background: var(--kavak-card);
    border: 1px solid var(--kavak-border);
    border-radius: 10px;
    overflow-x: auto;
}}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
thead th {{
    background: rgba(4,103,252,0.08);
    padding: 10px 10px;
    text-align: left;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--kavak-text-sec);
    border-bottom: 1px solid var(--kavak-border);
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
}}
thead th:hover {{ color: var(--kavak-blue); }}
thead th.sorted-asc::after {{ content: ' \\25B2'; font-size: 8px; }}
thead th.sorted-desc::after {{ content: ' \\25BC'; font-size: 8px; }}
tbody td {{
    padding: 7px 10px;
    border-bottom: 1px solid var(--kavak-border);
    white-space: nowrap;
}}
tbody tr:hover {{ background: rgba(4,103,252,0.05); }}
tbody tr.totals-row {{ background: rgba(4,103,252,0.06); font-weight: 700; }}
tbody tr.totals-row td {{ border-top: 2px solid var(--kavak-blue); }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-family: 'SF Mono', 'Fira Code', 'Inter', monospace; font-size: 11px; }}
td.campaign-name {{ font-weight: 600; min-width: 130px; }}
td.efficiency {{
    font-weight: 800;
    color: var(--kavak-blue);
    font-size: 12px;
}}
.change-pill {{
    display: inline-block;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 700;
}}
.change-pill.positive {{ background: rgba(0,196,140,0.15); color: var(--kavak-green); }}
.change-pill.negative {{ background: rgba(255,71,87,0.15); color: var(--kavak-red); }}
.change-pill.neutral {{ background: rgba(255,255,255,0.05); color: var(--kavak-text-sec); }}
.top-badge {{ background: rgba(0,196,140,0.2); color: var(--kavak-green); padding: 1px 5px; border-radius: 3px; font-size: 9px; font-weight: 700; margin-left: 4px; }}
.low-badge {{ background: rgba(255,71,87,0.2); color: var(--kavak-red); padding: 1px 5px; border-radius: 3px; font-size: 9px; font-weight: 700; margin-left: 4px; }}

/* ── Cell WoW context ── */
td.wow-up {{ background: rgba(0,196,140,0.08); }}
td.wow-down {{ background: rgba(255,71,87,0.08); }}
td.wow-up-inv {{ background: rgba(255,71,87,0.08); }}  /* inverted: up is bad (CPOS, MD) */
td.wow-down-inv {{ background: rgba(0,196,140,0.08); }}  /* inverted: down is good */
td[data-wow] {{ cursor: default; position: relative; }}

/* ── Insights ── */
.insights-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 10px; }}
.insight {{
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 500;
    border-left: 3px solid;
}}
.insight.positive {{ background: rgba(0,196,140,0.08); border-color: var(--kavak-green); }}
.insight.negative {{ background: rgba(255,71,87,0.08); border-color: var(--kavak-red); }}
.insight.info {{ background: rgba(4,103,252,0.08); border-color: var(--kavak-blue); }}

/* ── Watermark & Footer ── */
.watermark {{
    position: fixed;
    bottom: 12px;
    right: 16px;
    display: flex;
    align-items: center;
    gap: 6px;
    opacity: 0.1;
    pointer-events: none;
}}
.watermark svg {{ width: 50px; height: auto; fill: #fff; }}
.watermark span {{ font-size: 9px; text-transform: uppercase; letter-spacing: 2px; color: #fff; font-weight: 600; }}
/* ── Combiner Panel ── */
.combiner-btn {{
    padding: 5px 14px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--kavak-border);
    background: transparent;
    color: var(--kavak-text-sec);
    transition: all 0.15s;
}}
.combiner-btn:hover {{ border-color: #A855F7; color: #A855F7; }}
.combiner-btn.active {{ background: #A855F7; border-color: #A855F7; color: #fff; }}
.combiner-overlay {{
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.75);
    z-index: 999;
    justify-content: center;
    align-items: flex-start;
    padding-top: 80px;
    backdrop-filter: blur(4px);
}}
.combiner-overlay.show {{ display: flex; }}
.combiner-panel {{
    background: #1e1e2e;
    border: 1px solid rgba(168,85,247,0.3);
    border-radius: 12px;
    padding: 24px 28px;
    width: 440px;
    max-height: 70vh;
    overflow-y: auto;
    box-shadow: 0 20px 50px rgba(0,0,0,0.6), 0 0 0 1px rgba(168,85,247,0.15);
}}
.combiner-panel h3 {{
    margin: 0 0 12px 0;
    font-size: 14px;
    color: var(--kavak-text);
}}
.combiner-group {{
    margin-bottom: 12px;
}}
.combiner-group-title {{
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--kavak-text-sec);
    margin-bottom: 6px;
}}
.combiner-item {{
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 0;
    cursor: pointer;
    font-size: 12px;
    color: var(--kavak-text);
}}
.combiner-item input[type="checkbox"] {{
    accent-color: #A855F7;
    width: 14px;
    height: 14px;
}}
.combiner-item .color-dot {{
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
}}
.combiner-actions {{
    display: flex;
    gap: 10px;
    margin-top: 16px;
    padding-top: 12px;
    border-top: 1px solid var(--kavak-border);
}}
.combiner-create {{
    flex: 1;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    border: none;
    background: #A855F7;
    color: #fff;
    transition: opacity 0.15s;
}}
.combiner-create:hover {{ opacity: 0.85; }}
.combiner-create:disabled {{ opacity: 0.4; cursor: default; }}
.combiner-cancel {{
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--kavak-border);
    background: transparent;
    color: var(--kavak-text-sec);
}}
.combiner-cancel:hover {{ border-color: var(--kavak-text); color: var(--kavak-text); }}

.footer {{
    text-align: center;
    padding: 24px;
    font-size: 10px;
    color: var(--kavak-text-sec);
    border-top: 1px solid var(--kavak-border);
    margin-top: 40px;
}}

@media (max-width: 1200px) {{
    .kpi-grid {{ grid-template-columns: repeat(4, 1fr); }}
}}
@media (max-width: 900px) {{
    .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .charts-grid {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>

<!-- Header with Filters -->
<div class="header">
    <div class="header-top">
        <div class="header-left">
            <div class="header-logo">
                <svg viewBox="0 0 7774 2048"><path d="M7296.82.052L6752.798 1024l544.022 1023.948h477.424L7239.034 1024 7774.244.052zm-1130.746 0v1705.534L5275.298.052 4205.476 2047.954h470.514l599.916-1147.71 254.406 487.47h-254.406l-178.412 341.108h611.236l166.464 319.132h726.816V.052h-435.96zm-1767.734 0l-599.916 1147.71L3199.138.052h-470.514l1069.822 2047.902L4868.268.052H4398.39zm-2076.172 0l-892.04 1707.424L1072.7 1024 1607.91.052h-477.424L586.464 1024l544.022 1023.948h593.006l166.464-319.132h611.236l-178.412-341.108h-254.406l254.406-487.47 598.678 1147.71h470.514L2322.15.046zM-.244 2047.952h435.33V.05H-.244z"/></svg>
            </div>
            <div>
                <div class="header-title">Supply CRM Campaign Dashboard</div>
                <div class="header-subtitle">Lifecycle MX &middot; WA + Email + L2 Attribution</div>
            </div>
        </div>
        <div class="header-meta">
            <div>Generated: <span id="gen-time"></span></div>
            <div>Period: <span id="gen-range"></span></div>
        </div>
    </div>
    <div class="filter-bar">
        <div class="filter-group">
            <span class="filter-label">Channel:</span>
            <button class="pill active-wa" data-filter="channel" data-value="whatsapp" onclick="setFilter('channel','whatsapp',this)">WA</button>
            <button class="pill" data-filter="channel" data-value="email" onclick="setFilter('channel','email',this)">Email</button>
            <button class="pill" data-filter="channel" data-value="both" onclick="setFilter('channel','both',this)">Both</button>
        </div>
        <div class="filter-group">
            <span class="filter-label">Vertical:</span>
            <button class="pill active" data-filter="vertical" data-value="supply" onclick="setFilter('vertical','supply',this)">Supply Only</button>
            <button class="pill" data-filter="vertical" data-value="spillover" onclick="setFilter('vertical','spillover',this)">With Spillover</button>
        </div>
        <div class="filter-group">
            <span class="filter-label">Time:</span>
            <button class="pill active" data-filter="time" data-value="week" onclick="setFilter('time','week',this)">Week</button>
            <button class="pill" data-filter="time" data-value="month" onclick="setFilter('time','month',this)">Month</button>
            <button class="pill" data-filter="time" data-value="quarter" onclick="setFilter('time','quarter',this)">Quarter</button>
        </div>
        <div class="filter-group">
            <span class="filter-label">Range:</span>
            <button class="pill" data-filter="range" data-value="4" onclick="setFilter('range','4',this)">4W</button>
            <button class="pill" data-filter="range" data-value="8" onclick="setFilter('range','8',this)">8W</button>
            <button class="pill" data-filter="range" data-value="12" onclick="setFilter('range','12',this)">12W</button>
            <button class="pill active" data-filter="range" data-value="all" onclick="setFilter('range','all',this)">All</button>
        </div>
        <div class="filter-group">
            <span class="filter-label">Period:</span>
            <select class="period-select" id="period-select" onchange="setPeriod(this.value)">
                <option value="latest">Latest</option>
            </select>
        </div>
        <div class="filter-group" style="margin-left:auto;">
            <button class="combiner-btn" id="combiner-toggle" onclick="toggleCombiner()">&#128202; Custom Chart</button>
        </div>
    </div>
    <div class="tab-bar" style="margin-top:12px;">
        <button class="tab-btn active" onclick="switchTab('supply', this)"><span class="tab-icon">📊</span> Supply Dashboard</button>
        <button class="tab-btn" onclick="switchTab('saturation', this)"><span class="tab-icon">🎯</span> Saturación & Overlap</button>
    </div>
</div>

<!-- ==================== TAB 1: SUPPLY DASHBOARD ==================== -->
<div class="main tab-content active" id="tab-supply">
    <!-- KPI Cards -->
    <div class="section">
        <div class="section-title">Key Metrics <span id="kpi-channel-tag" class="channel-tag wa">WA</span></div>
        <div class="kpi-grid" id="kpi-grid"></div>
    </div>

    <!-- Trend Charts -->
    <div class="section">
        <div class="section-title">Trends Over Time</div>
        <div class="charts-grid">
            <div class="chart-box" onclick="openModal('volumes')">
                <h3>Volume: Deliveries &amp; OS <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-volumes"></canvas></div>
            </div>
            <div class="chart-box" onclick="openModal('rates')">
                <h3>Rates: OR% (left) &middot; CTR% &middot; CVR% (right) <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-rates"></canvas></div>
            </div>
        </div>
    </div>

    <!-- Campaign Evolution -->
    <div class="section">
        <div class="section-title">Campaign OS Evolution <span id="evo-table-tag" class="channel-tag wa">WA</span></div>
        <div class="chart-box" onclick="openModal('campaignEvo')" style="cursor:pointer">
            <h3>OS by Campaign Type Over Time <span class="expand-hint">Click to expand</span></h3>
            <div class="chart-container" style="height:300px"><canvas id="chart-campaign-evo"></canvas></div>
        </div>
    </div>

    <!-- Campaign Type Table -->
    <div class="section">
        <div class="section-title">Campaign Type Breakdown <span id="type-table-tag" class="channel-tag wa">WA</span></div>
        <div class="table-wrap">
            <table id="type-table">
                <thead><tr id="type-thead"></tr></thead>
                <tbody id="type-tbody"></tbody>
            </table>
        </div>
    </div>

    <!-- Value Prop Repetition (Pulse WA) -->
    <div class="section">
        <div class="section-title">Value Prop Repetition: Variedad de propuesta por usuario <span class="channel-tag wa">WA Supply</span></div>
        <div style="font-size:11px;color:var(--kavak-text-sec);margin:-8px 0 12px 0;">
            Para cada usuario Pulse con WA Supply, se identifica su VP principal (m&aacute;s enviado esa semana). Repetici&oacute;n = mismo VP que la semana anterior.
        </div>
        <div class="charts-grid">
            <div class="chart-box" onclick="openModal('vpMix')" style="cursor:pointer">
                <h3>Value Prop Mix (%% Share by Week) <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-vp-mix"></canvas></div>
            </div>
            <div class="chart-box" onclick="openModal('vpRepetition')" style="cursor:pointer">
                <h3>VP Repetition Rate (%% Same VP as Prev Week) <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-vp-repetition"></canvas></div>
            </div>
        </div>
    </div>

    <!-- Campaign Detail Table -->
    <div class="section">
        <div class="section-title">Campaign Detail <span id="detail-table-tag" class="channel-tag wa">WA</span></div>
        <div class="table-wrap">
            <table id="detail-table">
                <thead><tr id="detail-thead"></tr></thead>
                <tbody id="detail-tbody"></tbody>
            </table>
        </div>
    </div>

    <!-- Auto-Insights -->
    <div class="section" id="insights-section" style="display:none;">
        <div class="section-title">Auto-Insights</div>
        <div class="insights-grid" id="insights-grid"></div>
    </div>
</div>

<!-- ==================== TAB 2: SATURACIÓN & OVERLAP ==================== -->
<div class="main tab-content" id="tab-saturation">
    <!-- Pulse Type Filter -->
    <div class="section" style="margin-bottom:16px;">
        <div class="filter-bar">
            <div class="filter-group">
                <span class="filter-label">Pulse Type:</span>
                <div id="sat-filter-pills"><!-- populated dynamically --></div>
            </div>
        </div>
    </div>

    <!-- Saturation KPIs -->
    <div class="section">
        <div class="section-title">WA Saturation — Pulse Audience MX <span class="channel-tag wa">WA All Verticals</span></div>
        <div style="font-size:11px;color:var(--kavak-text-sec);margin:-8px 0 12px 0;">Audiencia = usuarios en <code>pulse_details</code> (journey Supply). WA contados = de TODAS las verticales que les llegan.</div>
        <div class="kpi-grid" style="grid-template-columns: repeat(5, 1fr);" id="sat-kpi-grid"></div>
    </div>

    <!-- Frequency Trend + Distribution -->
    <div class="section">
        <div class="section-title">WA Frequency: Total WA recibidos por la audiencia Pulse (todas las verticales)</div>
        <div class="charts-grid">
            <div class="chart-box" onclick="openModal('freqTrend')">
                <h3>Avg WA Sends / User & %% Impacted <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-freq-trend"></canvas></div>
            </div>
            <div class="chart-box" onclick="openModal('freqDist')">
                <h3>Frequency Distribution (Users by # WA) <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-freq-dist"></canvas></div>
            </div>
        </div>
    </div>

    <!-- Vertical Overlap Charts -->
    <div class="section">
        <div class="section-title">Vertical Overlap: De qu&eacute; vertical viene cada WA (categorias exclusivas)</div>
        <div style="font-size:11px;color:var(--kavak-text-sec);margin:-8px 0 12px 0;">Clasificaci&oacute;n por UTM: <code>purchase</code>=Supply, <code>kuna</code>=AE, <code>sale</code>=Sales. Categorias mutuamente exclusivas (sin double-counting).</div>
        <div class="charts-grid">
            <div class="chart-box" onclick="openModal('overlapStack')">
                <h3>Users by Vertical Mix (Exclusive Categories) <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-overlap-stack"></canvas></div>
            </div>
            <div class="chart-box" onclick="openModal('overlapPct')">
                <h3>%% Overlap Trend by Vertical Combo <span class="expand-hint">Click to expand</span></h3>
                <div class="chart-container"><canvas id="chart-overlap-pct"></canvas></div>
            </div>
        </div>
    </div>

    <!-- Saturation Table -->
    <div class="section">
        <div class="section-title">Weekly Detail</div>
        <div class="table-wrap">
            <table id="sat-table">
                <thead><tr id="sat-thead"></tr></thead>
                <tbody id="sat-tbody"></tbody>
            </table>
        </div>
    </div>
</div>

<!-- Modal for expanded charts -->
<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
    <div class="modal-content" onclick="event.stopPropagation()">
        <button class="modal-close" onclick="closeModalForce()">&times;</button>
        <div class="modal-chart"><canvas id="modal-canvas"></canvas></div>
        <div class="legend-hint">💡 Click on legend items to show/hide lines</div>
    </div>
</div>

<!-- Combiner Panel -->
<div class="combiner-overlay" id="combiner-overlay" onclick="if(event.target===this)closeCombiner()">
    <div class="combiner-panel" onclick="event.stopPropagation()">
        <h3>&#128202; Custom Chart Builder</h3>
        <div id="combiner-metrics"></div>
        <div class="combiner-actions">
            <button class="combiner-create" id="combiner-create-btn" onclick="createCombinedChart()" disabled>Create Chart</button>
            <button class="combiner-cancel" onclick="closeCombiner()">Cancel</button>
        </div>
    </div>
</div>

<!-- Watermark -->
<div class="watermark">
    <svg viewBox="0 0 7774 2048"><path d="M7296.82.052L6752.798 1024l544.022 1023.948h477.424L7239.034 1024 7774.244.052zm-1130.746 0v1705.534L5275.298.052 4205.476 2047.954h470.514l599.916-1147.71 254.406 487.47h-254.406l-178.412 341.108h611.236l166.464 319.132h726.816V.052h-435.96zm-1767.734 0l-599.916 1147.71L3199.138.052h-470.514l1069.822 2047.902L4868.268.052H4398.39zm-2076.172 0l-892.04 1707.424L1072.7 1024 1607.91.052h-477.424L586.464 1024l544.022 1023.948h593.006l166.464-319.132h611.236l-178.412-341.108h-254.406l254.406-487.47 598.678 1147.71h470.514L2322.15.046zM-.244 2047.952h435.33V.05H-.244z"/></svg>
    <span>Confidential</span>
</div>

<div class="footer">
    Supply CRM Campaign Dashboard v2 &middot; Kavak Lifecycle MX &middot; Generated by Claude
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// RAW DATA (embedded by Python)
// ════════════════════════════════════════════════════════════════════════════
const RAW = {data_json};

// ════════════════════════════════════════════════════════════════════════════
// STATE
// ════════════════════════════════════════════════════════════════════════════
// Ensure numeric — Redshift ROUND() sometimes returns strings
function N(v) {{ return parseFloat(v) || 0; }}

const STATE = {{
    channel: 'whatsapp',   // 'whatsapp', 'email', 'both'
    vertical: 'supply',    // 'supply', 'spillover'
    time: 'week',          // 'week', 'month', 'quarter'
    range: 'all',          // 'all', '4', '8', '12'
    selectedPeriod: null,  // null = latest
}};

// Chart instances for cleanup
let chartInstances = {{}};
let modalChart = null;
let currentModalType = null;

// ════════════════════════════════════════════════════════════════════════════
// FILTER LOGIC
// ════════════════════════════════════════════════════════════════════════════

function setFilter(group, value, btn) {{
    STATE[group] = value;
    // Update pill styles
    document.querySelectorAll(`[data-filter="${{group}}"]`).forEach(p => {{
        p.className = 'pill';
    }});
    if (group === 'channel') {{
        if (value === 'whatsapp') btn.className = 'pill active-wa';
        else if (value === 'email') btn.className = 'pill active-email';
        else btn.className = 'pill active';
    }} else {{
        btn.className = 'pill active';
    }}
    // Reset period selection when time grouping changes, update range labels
    if (group === 'time') {{
        STATE.selectedPeriod = null;
        updateRangeLabels();
    }}
    renderAll();
}}

function setPeriod(value) {{
    STATE.selectedPeriod = value === 'latest' ? null : value;
    renderAll();
}}

function populatePeriodDropdown(buckets) {{
    const sel = document.getElementById('period-select');
    if (!sel) return;
    const prev = sel.value;
    sel.innerHTML = '<option value="latest">Latest</option>';
    // Show periods in reverse order (most recent first)
    [...buckets].reverse().forEach(b => {{
        const opt = document.createElement('option');
        opt.value = b;
        opt.textContent = timeLabel(b);
        sel.appendChild(opt);
    }});
    // Restore selection if still valid
    if (STATE.selectedPeriod && buckets.includes(STATE.selectedPeriod)) {{
        sel.value = STATE.selectedPeriod;
    }} else {{
        sel.value = 'latest';
        STATE.selectedPeriod = null;
    }}
}}

function updateRangeLabels() {{
    const suffix = STATE.time === 'week' ? 'W' : STATE.time === 'month' ? 'M' : 'Q';
    document.querySelectorAll('[data-filter="range"]').forEach(btn => {{
        const val = btn.getAttribute('data-value');
        if (val !== 'all') btn.textContent = val + suffix;
    }});
}}

function channelLabel() {{
    if (STATE.channel === 'whatsapp') return 'WA';
    if (STATE.channel === 'email') return 'Email';
    return 'All';
}}

function channelTagClass() {{
    if (STATE.channel === 'whatsapp') return 'wa';
    if (STATE.channel === 'email') return 'email';
    return 'both';
}}

function verticalFilter() {{
    if (STATE.vertical === 'supply') return ['Supply'];
    return null; // null = no filter, include all verticals (Supply + Sales + Auto Equity + Other)
}}

function mediumToChannel(medium) {{
    if (!medium) return 'unknown';
    const m = medium.toLowerCase();
    if (m === 'architect_whatsapp') return 'whatsapp';
    if (m === 'architect_email' || m === 'email') return 'email';
    return 'unknown';
}}

function periodLabel() {{
    if (STATE.time === 'month') return 'MoM';
    if (STATE.time === 'quarter') return 'QoQ';
    return 'WoW';
}}

function avgLabel() {{
    if (STATE.time === 'month') return 'AVG L8M';
    if (STATE.time === 'quarter') return 'AVG L8Q';
    return 'AVG L8W';
}}

function matchChannel(ch) {{
    if (STATE.channel === 'both') return ch === 'whatsapp' || ch === 'email';
    return ch === STATE.channel;
}}

// ════════════════════════════════════════════════════════════════════════════
// TIME AGGREGATION
// ════════════════════════════════════════════════════════════════════════════

function timeBucket(dateStr) {{
    const d = new Date(dateStr + 'T00:00:00');
    if (STATE.time === 'week') return dateStr;
    if (STATE.time === 'month') {{
        return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0');
    }}
    // Quarter
    const q = Math.ceil((d.getMonth()+1)/3);
    return d.getFullYear() + '-Q' + q;
}}

function timeLabel(bucket) {{
    if (STATE.time === 'week') {{
        const d = new Date(bucket + 'T00:00:00');
        const oneJan = new Date(d.getFullYear(), 0, 1);
        const weekNum = Math.ceil((((d - oneJan) / 86400000) + oneJan.getDay() + 1) / 7);
        return 'W' + String(weekNum).padStart(2, '0');
    }}
    if (STATE.time === 'month') {{
        const parts = bucket.split('-');
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return months[parseInt(parts[1])-1] + " '" + parts[0].slice(2);
    }}
    return bucket;
}}

function isWtd(bucket) {{
    if (!RAW.has_wtd || STATE.time !== 'week') return false;
    // Check if this bucket is the latest week
    const allWeeks = new Set();
    (RAW.engagement_all || []).forEach(r => allWeeks.add(r.week));
    const sorted = [...allWeeks].sort();
    return sorted.length > 0 && bucket === sorted[sorted.length - 1];
}}

// ════════════════════════════════════════════════════════════════════════════
// DATA AGGREGATION FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════

function aggregateEngagement(data) {{
    // Returns Map<bucket, {{deliveries, unique_users, opens, clicks}}>
    const map = new Map();
    (data || []).forEach(r => {{
        if (!matchChannel(r.channel)) return;
        const b = timeBucket(r.week);
        if (!map.has(b)) map.set(b, {{deliveries:0, unique_users:0, opens:0, clicks:0}});
        const m = map.get(b);
        m.deliveries += (r.deliveries || 0);
        m.unique_users += (r.unique_users || 0);
        m.opens += (r.opens || 0);
        m.clicks += (r.clicks || 0);
    }});
    return map;
}}

function aggregateEngagementByChannel(data) {{
    // Returns Map<bucket, Map<channel, {{deliveries, opens, clicks}}>>
    const map = new Map();
    (data || []).forEach(r => {{
        if (r.channel !== 'whatsapp' && r.channel !== 'email') return;
        const b = timeBucket(r.week);
        if (!map.has(b)) map.set(b, new Map());
        const ch = map.get(b);
        if (!ch.has(r.channel)) ch.set(r.channel, {{deliveries:0, opens:0, clicks:0}});
        const m = ch.get(r.channel);
        m.deliveries += (r.deliveries || 0);
        m.opens += (r.opens || 0);
        m.clicks += (r.clicks || 0);
    }});
    return map;
}}

function aggregateOS(data) {{
    // Returns Map<bucket, number> filtered by vertical and channel
    // OS data now has 'channel' pre-mapped in SQL (whatsapp/email/other)
    const verts = verticalFilter();
    const map = new Map();
    (data || []).forEach(r => {{
        if (verts && !verts.includes(r.vertical)) return;
        const ch = r.channel || 'other';
        if (!matchChannel(ch)) return;
        const b = timeBucket(r.week);
        map.set(b, (map.get(b) || 0) + (r.os || 0));
    }});
    return map;
}}

function aggregateOSByChannel(data) {{
    // Returns Map<bucket, Map<channel, number>> for both channels
    const verts = verticalFilter();
    const map = new Map();
    (data || []).forEach(r => {{
        if (verts && !verts.includes(r.vertical)) return;
        const ch = r.channel || 'other';
        if (ch !== 'whatsapp' && ch !== 'email') return;
        const b = timeBucket(r.week);
        if (!map.has(b)) map.set(b, new Map());
        const chMap = map.get(b);
        chMap.set(ch, (chMap.get(ch) || 0) + (r.os || 0));
    }});
    return map;
}}

function aggregateEngByGroup(data, groupKey) {{
    // Returns Map<groupValue, Map<bucket, {{deliveries, opens, clicks, promo_deliveries}}>>
    const result = new Map();
    (data || []).forEach(r => {{
        if (!matchChannel(r.channel)) return;
        const g = r[groupKey] || 'Others';
        const b = timeBucket(r.week);
        if (!result.has(g)) result.set(g, new Map());
        const bMap = result.get(g);
        if (!bMap.has(b)) bMap.set(b, {{deliveries:0, opens:0, clicks:0, promo_deliveries:0}});
        const m = bMap.get(b);
        m.deliveries += (r.deliveries || 0);
        m.opens += (r.opens || 0);
        m.clicks += (r.clicks || 0);
        m.promo_deliveries += (r.promo_deliveries || 0);
    }});
    return result;
}}

function aggregateOSByGroup(data, groupKey) {{
    // Returns Map<groupValue, Map<bucket, number>>
    const verts = verticalFilter();
    const result = new Map();
    (data || []).forEach(r => {{
        if (verts && !verts.includes(r.vertical)) return;
        const ch = r.channel || 'other';
        if (!matchChannel(ch)) return;
        const g = r[groupKey] || 'Others';
        const b = timeBucket(r.week);
        if (!result.has(g)) result.set(g, new Map());
        const bMap = result.get(g);
        bMap.set(b, (bMap.get(b) || 0) + (r.os || 0));
    }});
    return result;
}}

function aggregateDeliveriesByChannel(data) {{
    // Returns Map<bucket, {{wa_del, email_del}}> for CPOS cost calculation
    const map = new Map();
    (data || []).forEach(r => {{
        const b = timeBucket(r.week);
        if (!map.has(b)) map.set(b, {{wa_del:0, email_del:0}});
        const m = map.get(b);
        if (r.channel === 'whatsapp') m.wa_del += (r.deliveries || 0);
        if (r.channel === 'email') m.email_del += (r.deliveries || 0);
    }});
    return map;
}}

function aggregateMD(data) {{
    // Returns Map<bucket, {{sum_md, count}}>  (for weighted avg)
    const map = new Map();
    (data || []).forEach(r => {{
        const b = timeBucket(r.week);
        if (!map.has(b)) map.set(b, {{sum: 0, count: 0}});
        const m = map.get(b);
        m.sum += (r.avg_md_pct || 0) * (r.registers || 1);
        m.count += (r.registers || 1);
    }});
    return map;
}}

// ════════════════════════════════════════════════════════════════════════════
// FORMATTING HELPERS
// ════════════════════════════════════════════════════════════════════════════

function fmtNum(v) {{
    if (v === null || v === undefined || isNaN(v)) return '--';
    v = Math.round(v);
    if (Math.abs(v) >= 1000000) return (v/1000000).toFixed(1) + 'M';
    if (Math.abs(v) >= 10000) return (v/1000).toFixed(1) + 'K';
    return v.toLocaleString('es-MX');
}}

function fmtPct(v) {{
    if (v === null || v === undefined || isNaN(v)) return '--';
    return v.toFixed(2) + '%';
}}

function fmtMoney(v) {{
    if (v === null || v === undefined || isNaN(v)) return '--';
    return '$' + v.toFixed(2);
}}

function fmtEff(v) {{
    if (v === null || v === undefined || isNaN(v)) return '--';
    return v.toFixed(2);
}}

function safePct(num, den) {{
    if (!den || den === 0) return 0;
    return (num / den) * 100;
}}

function safeDiv(num, den) {{
    if (!den || den === 0) return 0;
    return num / den;
}}

function wowChange(current, previous) {{
    if (!previous || previous === 0) return null;
    return ((current - previous) / Math.abs(previous)) * 100;
}}

function changePillHtml(val) {{
    if (val === null || val === undefined) return '<span class="change-pill neutral">--</span>';
    const cls = val > 0 ? 'positive' : val < 0 ? 'negative' : 'neutral';
    const sign = val > 0 ? '+' : '';
    return `<span class="change-pill ${{cls}}">${{sign}}${{val.toFixed(1)}}%</span>`;
}}

// Cell context: returns class + title for a metric cell based on WoW change
// inverted=true for metrics where lower is better (CPOS, MD)
// isPp=true for metrics where the change is in percentage points, not %
function wowCellAttr(val, inverted, isPp) {{
    if (val === null || val === undefined || val === 0) return '';
    const sign = val > 0 ? '+' : '';
    const unit = isPp ? 'pp' : '%';
    const tip = `${{sign}}${{isPp ? val.toFixed(2) : val.toFixed(1)}}${{unit}} ${{periodLabel()}}`;
    let cls;
    if (inverted) {{
        cls = val > 0 ? 'wow-up-inv' : 'wow-down-inv';
    }} else {{
        cls = val > 0 ? 'wow-up' : 'wow-down';
    }}
    return ` class="num ${{cls}}" title="${{tip}}" data-wow="${{val.toFixed(1)}}"`;
}}

function yAxisCallback(value) {{
    if (Math.abs(value) >= 1000000) return (value / 1000000).toFixed(1) + 'M';
    if (Math.abs(value) >= 10000) return (value / 1000).toFixed(0) + 'K';
    if (Math.abs(value) >= 1000) return (value / 1000).toFixed(1) + 'K';
    return value;
}}

function yAxisCallbackPct(value) {{
    return value.toFixed(1) + '%';
}}

// ════════════════════════════════════════════════════════════════════════════
// SORTED BUCKETS (time periods)
// ════════════════════════════════════════════════════════════════════════════

function getAllBuckets() {{
    const buckets = new Set();
    (RAW.engagement_all || []).forEach(r => buckets.add(timeBucket(r.week)));
    (RAW.os_weekly || []).forEach(r => buckets.add(timeBucket(r.week)));
    (RAW.md_weekly || []).forEach(r => buckets.add(timeBucket(r.week)));
    let sorted = [...buckets].sort();
    // Exclude current incomplete period (month/quarter)
    if (STATE.time === 'month') {{
        const now = new Date();
        const curMonth = now.getFullYear() + '-' + String(now.getMonth()+1).padStart(2,'0');
        sorted = sorted.filter(b => b < curMonth);
    }} else if (STATE.time === 'quarter') {{
        const now = new Date();
        const curQ = now.getFullYear() + '-Q' + Math.ceil((now.getMonth()+1)/3);
        sorted = sorted.filter(b => b < curQ);
    }}
    return sorted;
}}

function getSortedBuckets() {{
    let sorted = getAllBuckets();
    // Apply range filter
    if (STATE.range !== 'all') {{
        const n = parseInt(STATE.range);
        if (n > 0 && sorted.length > n) {{
            sorted = sorted.slice(-n);
        }}
    }}
    return sorted;
}}

// ════════════════════════════════════════════════════════════════════════════
// CHART UTILS
// ════════════════════════════════════════════════════════════════════════════

function destroyChart(id) {{
    if (chartInstances[id]) {{
        chartInstances[id].destroy();
        delete chartInstances[id];
    }}
}}

function createChart(canvasId, config) {{
    destroyChart(canvasId);
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    const ctx = canvas.getContext('2d');
    const chart = new Chart(ctx, config);
    chartInstances[canvasId] = chart;
    return chart;
}}

function createSparkline(canvas, values, color) {{
    if (!values || values.length === 0) return;
    new Chart(canvas, {{
        type: 'line',
        data: {{
            labels: values.map((_, i) => i),
            datasets: [{{
                data: values,
                borderColor: color || '#0467FC',
                borderWidth: 1.5,
                fill: false,
                pointRadius: 0,
                tension: 0.3,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
            scales: {{ x: {{ display: false }}, y: {{ display: false }} }}
        }}
    }});
}}

// ── Plugin: highlight selected period on all charts ──
const periodHighlightPlugin = {{
    id: 'periodHighlight',
    beforeDraw(chart) {{
        if (!STATE.selectedPeriod) return;
        const xScale = chart.scales.x;
        if (!xScale) return;
        const labels = chart.data.labels || [];
        const selLabel = timeLabel(STATE.selectedPeriod);
        const idx = labels.indexOf(selLabel);
        if (idx < 0) return;

        const ctx = chart.ctx;
        const meta = chart.getDatasetMeta(0);
        if (!meta || !meta.data || !meta.data[idx]) return;

        const x = meta.data[idx].x;
        const halfW = labels.length > 1 && meta.data.length > 1
            ? Math.abs(meta.data[Math.min(idx+1, meta.data.length-1)].x - meta.data[Math.max(idx-1, 0)].x) / (idx === 0 || idx === meta.data.length-1 ? 2 : 2)
            : 20;
        const top = chart.chartArea.top;
        const bottom = chart.chartArea.bottom;

        ctx.save();
        ctx.fillStyle = 'rgba(168, 85, 247, 0.12)';
        ctx.fillRect(x - halfW/2, top, halfW, bottom - top);
        // Small label at top
        ctx.fillStyle = 'rgba(168, 85, 247, 0.6)';
        ctx.font = '9px Inter';
        ctx.textAlign = 'center';
        ctx.fillText('▼', x, top + 8);
        ctx.restore();
    }}
}};
Chart.register(periodHighlightPlugin);

const CHART_DEFAULTS = {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{
        legend: {{
            display: true,
            position: 'top',
            labels: {{
                color: 'rgba(255,255,255,0.7)',
                font: {{ family: 'Inter', size: 10 }},
                boxWidth: 12,
                padding: 12,
            }}
        }},
        tooltip: {{
            backgroundColor: '#1B2532',
            titleFont: {{ family: 'Inter', size: 11 }},
            bodyFont: {{ family: 'Inter', size: 11 }},
            borderColor: 'rgba(255,255,255,0.1)',
            borderWidth: 1,
            padding: 10,
            callbacks: {{
                label: function(ctx) {{
                    const val = ctx.parsed.y;
                    const idx = ctx.dataIndex;
                    const data = ctx.dataset.data;
                    let label = ctx.dataset.label || '';
                    // Format value
                    let valStr;
                    if (Math.abs(val) >= 1000000) valStr = (val/1000000).toFixed(1) + 'M';
                    else if (Math.abs(val) >= 1000) valStr = (val/1000).toFixed(1) + 'K';
                    else if (Number.isInteger(val)) valStr = val.toString();
                    else valStr = val.toFixed(2);
                    // Add % suffix for rate datasets
                    if (label.includes('%') || label.includes('OR') || label.includes('CTR') || label.includes('CVR')) valStr = val.toFixed(2) + '%';
                    let result = label + ': ' + valStr;
                    // Compute vs previous period
                    if (idx > 0) {{
                        const prev = data[idx - 1];
                        if (prev && prev !== 0) {{
                            const pctChg = ((val - prev) / Math.abs(prev) * 100).toFixed(1);
                            const sign = pctChg > 0 ? '+' : '';
                            result += '  (' + sign + pctChg + '% ' + periodLabel() + ')';
                        }}
                    }}
                    return result;
                }}
            }}
        }}
    }},
    scales: {{
        x: {{
            ticks: {{ color: 'rgba(255,255,255,0.5)', font: {{ family: 'Inter', size: 10 }} }},
            grid: {{ color: 'rgba(255,255,255,0.04)' }},
        }},
        y: {{
            ticks: {{
                color: 'rgba(255,255,255,0.5)',
                font: {{ family: 'Inter', size: 10 }},
                callback: yAxisCallback,
            }},
            grid: {{ color: 'rgba(255,255,255,0.04)' }},
        }}
    }}
}};

// ════════════════════════════════════════════════════════════════════════════
// METRIC COMBINER — Registry & Logic
// ════════════════════════════════════════════════════════════════════════════

const METRIC_REGISTRY = [
    // ── Volumes ──
    {{ id: 'deliveries', label: 'Deliveries', group: 'Volumes', unit: 'number', color: '#0467FC',
       extract: (buckets) => {{
           const eng = aggregateEngagement(RAW.engagement_all);
           return buckets.map(b => {{ const e = eng.get(b); return e ? e.deliveries : 0; }});
       }}
    }},
    {{ id: 'os', label: 'OS', group: 'Volumes', unit: 'number', color: '#00C48C',
       extract: (buckets) => {{
           const osMap = aggregateOS(RAW.os_weekly);
           return buckets.map(b => osMap.get(b) || 0);
       }}
    }},
    {{ id: 'md', label: 'MD%', group: 'Volumes', unit: 'pct', color: '#FF4757',
       extract: (buckets) => {{
           const mdMap = aggregateMD(RAW.md_weekly);
           return buckets.map(b => {{ const m = mdMap.get(b); return m && m.count > 0 ? m.sum / m.count : null; }});
       }}
    }},
    // ── Rates ──
    {{ id: 'or', label: 'OR%', group: 'Rates', unit: 'pct', color: '#FF9F43',
       extract: (buckets) => {{
           const eng = aggregateEngagement(RAW.engagement_all);
           return buckets.map(b => {{ const e = eng.get(b); return e && e.deliveries > 0 ? safePct(e.opens, e.deliveries) : null; }});
       }}
    }},
    {{ id: 'ctr', label: 'CTR%', group: 'Rates', unit: 'pct', color: '#A855F7',
       extract: (buckets) => {{
           const eng = aggregateEngagement(RAW.engagement_all);
           return buckets.map(b => {{ const e = eng.get(b); return e && e.deliveries > 0 ? safePct(e.clicks, e.deliveries) : null; }});
       }}
    }},
    {{ id: 'cvr', label: 'CVR%', group: 'Rates', unit: 'pct', color: '#0467FC',
       extract: (buckets) => {{
           const eng = aggregateEngagement(RAW.engagement_all);
           const osMap = aggregateOS(RAW.os_weekly);
           return buckets.map(b => {{ const e = eng.get(b); const o = osMap.get(b) || 0; return e && e.deliveries > 0 ? safePct(o, e.deliveries) : null; }});
       }}
    }},
    // ── Efficiency ──
    {{ id: 'cpos', label: 'CPOS ($)', group: 'Efficiency', unit: 'money', color: '#FF9F43',
       extract: (buckets) => {{
           const delByCh = aggregateDeliveriesByChannel(RAW.engagement_all);
           const osMap = aggregateOS(RAW.os_weekly);
           return buckets.map(b => {{
               const dc = delByCh.get(b) || {{wa_del:0, email_del:0}};
               const o = osMap.get(b) || 0;
               let cost = 0;
               if (STATE.channel === 'whatsapp' || STATE.channel === 'both') cost += dc.wa_del * RAW.wa_cost;
               if (STATE.channel === 'email' || STATE.channel === 'both') cost += dc.email_del * RAW.email_cost;
               return o > 0 ? cost / o : null;
           }});
       }}
    }},
    {{ id: 'os_per_1k', label: 'OS/1K Del', group: 'Efficiency', unit: 'number', color: '#00C48C',
       extract: (buckets) => {{
           const eng = aggregateEngagement(RAW.engagement_all);
           const osMap = aggregateOS(RAW.os_weekly);
           return buckets.map(b => {{ const e = eng.get(b); const o = osMap.get(b) || 0; return e && e.deliveries > 0 ? (o / e.deliveries * 1000) : null; }});
       }}
    }},
    // ── Saturation ──
    {{ id: 'avg_wa_user', label: 'Avg WA/User', group: 'Saturation', unit: 'number', color: '#FF9F43',
       extract: (buckets) => {{
           const sat = aggSatFreq();
           return buckets.map(b => {{ const r = sat.find(s => timeBucket(s.week) === b); return r ? r.avg_sends_per_impacted : null; }});
       }}
    }},
    {{ id: 'pct_impacted', label: '% Impacted (WA)', group: 'Saturation', unit: 'pct', color: '#00C48C',
       extract: (buckets) => {{
           const sat = aggSatFreq();
           return buckets.map(b => {{ const r = sat.find(s => timeBucket(s.week) === b); return r ? r.pct_impacted : null; }});
       }}
    }},
];

let combinerSelections = new Set();

function buildCombinerPanel() {{
    const container = document.getElementById('combiner-metrics');
    if (!container) return;
    container.innerHTML = '';
    const groups = {{}};
    METRIC_REGISTRY.forEach(m => {{
        if (!groups[m.group]) groups[m.group] = [];
        groups[m.group].push(m);
    }});
    Object.keys(groups).forEach(g => {{
        const div = document.createElement('div');
        div.className = 'combiner-group';
        div.innerHTML = `<div class="combiner-group-title">${{g}}</div>`;
        groups[g].forEach(m => {{
            const item = document.createElement('label');
            item.className = 'combiner-item';
            item.innerHTML = `<input type="checkbox" value="${{m.id}}" ${{combinerSelections.has(m.id) ? 'checked' : ''}} onchange="toggleCombinerMetric('${{m.id}}', this.checked)"><span class="color-dot" style="background:${{m.color}}"></span>${{m.label}}`;
            div.appendChild(item);
        }});
        container.appendChild(div);
    }});
}}

function toggleCombinerMetric(id, checked) {{
    if (checked) combinerSelections.add(id);
    else combinerSelections.delete(id);
    const btn = document.getElementById('combiner-create-btn');
    if (btn) btn.disabled = combinerSelections.size < 2;
}}

function toggleCombiner() {{
    const overlay = document.getElementById('combiner-overlay');
    if (overlay.classList.contains('show')) {{
        closeCombiner();
    }} else {{
        buildCombinerPanel();
        overlay.classList.add('show');
        document.getElementById('combiner-toggle').classList.add('active');
    }}
}}

function closeCombiner() {{
    document.getElementById('combiner-overlay').classList.remove('show');
    document.getElementById('combiner-toggle').classList.remove('active');
}}

function createCombinedChart() {{
    closeCombiner();
    openModal('combiner');
}}

// ════════════════════════════════════════════════════════════════════════════
// CAMPAIGN TYPE DEFINITIONS (for hover tooltips)
// ════════════════════════════════════════════════════════════════════════════
const TYPE_DEFS = {{
    // Campaign types
    'Pulse': 'Main campaign — delivers the offer to users. Highest spend. Includes PriceAnch, Weekly Offer, Trade In (UTM: priceanch, weeklyoffer, wo_, tradein, cajati)',
    'Triggered': 'Automated lifecycle flows triggered by user actions (UTM: triggered, regnoos, osnomade, quotenoreg, offererror)',
    'Dormant Reac': 'Dormant reactivation — re-engaging users with register >1 year old (UTM: dormreac, dormant)',
    'Customers': 'Campaigns for users who already transacted. Similar to Pulse but specialized (UTM: customer)',
    'Onboarding': 'Welcome & onboarding flows for new registrations (UTM: onboarding, welcome)',
    'Email Cross': 'Email Journey series — multi-vertical email blasts. Includes Mystery Offer (UTM: emailjourney)',
    'Promos': 'Promotional campaigns — sorteos, concursos, bonos, and other incentive-driven sends (UTM: sorteo, concurso, promo, bono, inctrue)',
    'Others': 'All other campaigns not matching specific categories',
    // Detail subtypes
    'EC: Mystery Offer': 'Email Journey — gamified mystery offer emails (UTM: emailjourney + mystery)',
    'EC: Gen': 'Email Journey — generic supply emails (UTM: emailjourney)',
    'Trig: Reg No OS': 'Registered but no OS scheduled (UTM: regnoos)',
    'Trig: OS No Made': 'OS scheduled but not made (UTM: osnomade)',
    'Trig: Quot No Reg': 'Quoted but not registered (UTM: quotenoreg)',
    'Trig: Offer Error': 'Offer error recovery (UTM: offererror)',
    'Trig: Other': 'Other automated triggered flows (UTM: triggered)',
    'Pulse: Trade In': 'Trade-in targeting within Pulse (UTM: tradein, cajati)',
    'Pulse: WoW >5%': 'Price drop >5%% WoW notifications (UTM: cajawow)',
    'Pulse: WoW+MD': 'Combined WoW + Market Discount (UTM: cajawowmd)',
    'Pulse: MD <15%': 'Low market discount <15%% (UTM: cajamd)',
    'Pulse: Instant Offer': 'Instant offer messages (UTM: cajaofferinst, offerinst)',
    'Pulse: No Instant Offer': 'Users without instant offer (UTM: cajanofferinst)',
    'Pulse: Weekly Offer': 'Weekly offer emails — historically separate, now part of Pulse (UTM: weeklyoffer, wo_)',
    'Pulse: PriceAnch': 'Core price anchoring messages (UTM: priceanch, pulse)',
    'Promos: Sorteo': 'Raffle/sweepstakes campaigns (UTM: sorteo)',
    'Promos: Concurso': 'Contest campaigns (UTM: concurso)',
    'Promos: Other': 'Other promotional campaigns with incentives (UTM: promo, bono, inctrue)',
    'Onboarding': 'Welcome & onboarding flows (UTM: onboarding, welcome)',
}};

function typeTooltip(name) {{
    return TYPE_DEFS[name] || '';
}}

// TAB SWITCHING
// ════════════════════════════════════════════════════════════════════════════
let activeTab = 'supply';

function switchTab(tab, btn) {{
    activeTab = tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    if (tab === 'saturation') renderSaturationTab();
}}

// RENDER FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════

function renderAll() {{
    const buckets = getSortedBuckets();
    const labels = buckets.map(b => timeLabel(b));

    // Populate period dropdown with current buckets
    populatePeriodDropdown(buckets);

    // Update metadata
    document.getElementById('gen-time').textContent = RAW.generated_at || '';
    document.getElementById('gen-range').textContent = labels.length > 0 ? labels[0] + ' -- ' + labels[labels.length-1] : '';

    // Update channel tags
    const tagCls = channelTagClass();
    const tagText = channelLabel();
    ['kpi-channel-tag', 'type-table-tag', 'detail-table-tag'].forEach(id => {{
        const el = document.getElementById(id);
        if (el) {{
            el.className = 'channel-tag ' + tagCls;
            el.textContent = tagText;
        }}
    }});

    renderKPIs(buckets, labels);
    renderVolumeChart(buckets, labels);
    renderRatesChart(buckets, labels);
    renderCampaignEvo(buckets, labels);
    renderCampaignTypeTable(buckets, labels);
    renderCampaignDetailTable(buckets, labels);
    renderVPCharts();
    renderInsights(buckets);
    if (activeTab === 'saturation') renderSaturationTab();
}}

// ── KPIs ──
function renderKPIs(buckets, labels) {{
    const eng = aggregateEngagement(RAW.engagement_all);
    const osMap = aggregateOS(RAW.os_weekly);
    const mdMap = aggregateMD(RAW.md_weekly);
    const delByCh = aggregateDeliveriesByChannel(RAW.engagement_all);

    // Build arrays aligned to buckets
    const deliveries = [], opens = [], clicks = [], osArr = [], mdArr = [], cposArr = [];
    const wtdFlags = [];

    buckets.forEach(b => {{
        const e = eng.get(b) || {{deliveries:0, opens:0, clicks:0}};
        const o = osMap.get(b) || 0;
        const md = mdMap.get(b);
        const dc = delByCh.get(b) || {{wa_del:0, email_del:0}};

        deliveries.push(e.deliveries);
        opens.push(e.opens);
        clicks.push(e.clicks);
        osArr.push(o);

        // MD
        if (md && md.count > 0) mdArr.push(md.sum / md.count);
        else mdArr.push(null);

        // CPOS: cost from SELECTED channels / os from SELECTED vertical
        let cost = 0;
        if (STATE.channel === 'whatsapp' || STATE.channel === 'both') cost += dc.wa_del * RAW.wa_cost;
        if (STATE.channel === 'email' || STATE.channel === 'both') cost += dc.email_del * RAW.email_cost;
        cposArr.push(o > 0 ? cost / o : null);

        wtdFlags.push(isWtd(b));
    }});

    // Compute rates
    const orArr = deliveries.map((d, i) => d > 0 ? safePct(opens[i], d) : null);
    const ctrArr = deliveries.map((d, i) => d > 0 ? safePct(clicks[i], d) : null);
    const cvrArr = deliveries.map((d, i) => d > 0 ? safePct(osArr[i], d) : null);

    // Find the selected period (or last complete period)
    let lastIdx = buckets.length - 1;
    if (STATE.selectedPeriod) {{
        const selIdx = buckets.indexOf(STATE.selectedPeriod);
        if (selIdx >= 0) lastIdx = selIdx;
    }} else {{
        // Default: last complete period (exclude WTD if present)
        if (wtdFlags[lastIdx] && buckets.length >= 2) lastIdx = buckets.length - 2;
    }}
    const prevIdx = lastIdx - 1;

    // AVG L8W: average of up to 8 periods before current (excluding WTD)
    function avgRange(arr, endIdx) {{
        const start = Math.max(0, endIdx - 7);
        const slice = arr.slice(start, endIdx).filter(v => v !== null && v !== undefined);
        if (slice.length === 0) return null;
        return slice.reduce((a, b) => a + b, 0) / slice.length;
    }}

    const _avgLabel = avgLabel();

    // Sparkline data (exclude WTD period)
    const sparkEnd = wtdFlags[buckets.length - 1] ? buckets.length - 1 : buckets.length;

    const kpis = [
        {{ name: 'Deliveries', values: deliveries, fmt: 'num' }},
        {{ name: 'OS', values: osArr, fmt: 'num' }},
        {{ name: 'CVR (OS/Del)', values: cvrArr, fmt: 'pct' }},
        {{ name: 'OR%', values: orArr, fmt: 'pct' }},
        {{ name: 'CTR%', values: ctrArr, fmt: 'pct' }},
        {{ name: 'CPOS', values: cposArr, fmt: 'money' }},
        {{ name: 'MD', values: mdArr, fmt: 'pct' }},
    ];

    const grid = document.getElementById('kpi-grid');
    grid.innerHTML = '';

    kpis.forEach((kpi, idx) => {{
        const current = kpi.values[lastIdx];
        const prev = prevIdx >= 0 ? kpi.values[prevIdx] : null;
        const avg = avgRange(kpi.values, lastIdx);
        const wow = wowChange(current, prev);
        const vsAvg = wowChange(current, avg);
        const wtdVal = wtdFlags[buckets.length - 1] ? kpi.values[buckets.length - 1] : null;
        const sparkData = kpi.values.slice(0, sparkEnd);

        const card = document.createElement('div');
        card.className = 'kpi-card';

        let valStr;
        if (current === null || current === undefined) valStr = '--';
        else if (kpi.fmt === 'num') valStr = fmtNum(current);
        else if (kpi.fmt === 'pct') valStr = fmtPct(current);
        else if (kpi.fmt === 'money') valStr = fmtMoney(current);
        else valStr = String(current);

        let avgStr;
        if (avg === null || avg === undefined) avgStr = '--';
        else if (kpi.fmt === 'num') avgStr = fmtNum(avg);
        else if (kpi.fmt === 'pct') avgStr = fmtPct(avg);
        else if (kpi.fmt === 'money') avgStr = fmtMoney(avg);
        else avgStr = String(avg);

        const wowCls = wow !== null ? (wow > 0 ? 'positive' : wow < 0 ? 'negative' : '') : '';
        const wowStr = wow !== null ? (wow > 0 ? '+' : '') + wow.toFixed(1) + '%' : '--';
        const vsAvgCls = vsAvg !== null ? (vsAvg > 0 ? 'positive' : vsAvg < 0 ? 'negative' : '') : '';
        const vsAvgStr = vsAvg !== null ? (vsAvg > 0 ? '+' : '') + vsAvg.toFixed(1) + '%' : '--';

        // CPOS & MD: invert color logic (lower is better)
        let wowClsFinal = wowCls;
        let vsAvgClsFinal = vsAvgCls;
        if (kpi.name === 'CPOS' || kpi.name === 'MD') {{
            wowClsFinal = wow !== null ? (wow < 0 ? 'positive' : wow > 0 ? 'negative' : '') : '';
            vsAvgClsFinal = vsAvg !== null ? (vsAvg < 0 ? 'positive' : vsAvg > 0 ? 'negative' : '') : '';
        }}

        let wtdHtml = '';
        if (wtdVal !== null && wtdVal !== undefined) {{
            let wtdStr;
            if (kpi.fmt === 'num') wtdStr = fmtNum(wtdVal);
            else if (kpi.fmt === 'pct') wtdStr = fmtPct(wtdVal);
            else if (kpi.fmt === 'money') wtdStr = fmtMoney(wtdVal);
            else wtdStr = String(wtdVal);
            wtdHtml = `<div class="kpi-wtd">WTD: ${{wtdStr}}</div>`;
        }}

        card.innerHTML = `
            <div class="kpi-label">${{kpi.name}} <span class="channel-tag ${{channelTagClass()}}" style="font-size:8px;padding:1px 5px;">${{channelLabel()}}</span></div>
            <div class="kpi-value">${{valStr}}</div>
            <div class="kpi-meta">
                <span class="kpi-badge ${{wowClsFinal}}">${{periodLabel()}}: ${{wowStr}}</span>
                <span>${{avgLabel()}}: ${{avgStr}}</span>
                <span class="kpi-badge ${{vsAvgClsFinal}}">vs ${{avgLabel()}}: ${{vsAvgStr}}</span>
            </div>
            ${{wtdHtml}}
            <div class="kpi-sparkline"><canvas id="spark-${{idx}}"></canvas></div>
        `;
        grid.appendChild(card);

        // Render sparkline
        setTimeout(() => {{
            const sparkCanvas = document.getElementById('spark-' + idx);
            if (sparkCanvas) createSparkline(sparkCanvas, sparkData.filter(v => v !== null), kpi.name === 'CPOS' ? '#FF9F43' : '#0467FC');
        }}, 50);
    }});
}}

// ── Volume Chart ──
function renderVolumeChart(buckets, labels) {{
    const datasets = [];

    if (STATE.channel === 'both') {{
        // Separate lines per channel
        const engByCh = aggregateEngagementByChannel(RAW.engagement_all);
        const osByCh = aggregateOSByChannel(RAW.os_weekly);

        const waDelArr = [], emailDelArr = [], waOsArr = [], emailOsArr = [];
        buckets.forEach(b => {{
            const ch = engByCh.get(b);
            waDelArr.push(ch && ch.get('whatsapp') ? ch.get('whatsapp').deliveries : 0);
            emailDelArr.push(ch && ch.get('email') ? ch.get('email').deliveries : 0);
            const os = osByCh.get(b);
            waOsArr.push(os && os.get('whatsapp') ? os.get('whatsapp') : 0);
            emailOsArr.push(os && os.get('email') ? os.get('email') : 0);
        }});

        datasets.push(
            {{ label: 'Deliveries WA', data: waDelArr, borderColor: '#25D366', backgroundColor: 'rgba(37,211,102,0.1)', borderWidth: 2, fill: false, yAxisID: 'y', tension: 0.3 }},
            {{ label: 'Deliveries Email', data: emailDelArr, borderColor: '#FF9F43', backgroundColor: 'rgba(255,159,67,0.1)', borderWidth: 2, borderDash: [5,3], fill: false, yAxisID: 'y', tension: 0.3 }},
            {{ label: 'OS WA', data: waOsArr, borderColor: '#0467FC', backgroundColor: 'rgba(4,103,252,0.1)', borderWidth: 2, fill: false, yAxisID: 'y1', tension: 0.3 }},
            {{ label: 'OS Email', data: emailOsArr, borderColor: '#A855F7', backgroundColor: 'rgba(168,85,247,0.1)', borderWidth: 2, borderDash: [5,3], fill: false, yAxisID: 'y1', tension: 0.3 }}
        );
    }} else {{
        const eng = aggregateEngagement(RAW.engagement_all);
        const osMap = aggregateOS(RAW.os_weekly);
        const delArr = [], osArr = [];
        buckets.forEach(b => {{
            const e = eng.get(b) || {{deliveries:0}};
            delArr.push(e.deliveries);
            osArr.push(osMap.get(b) || 0);
        }});
        const chLabel = channelLabel();
        datasets.push(
            {{ label: 'Deliveries ' + chLabel, data: delArr, borderColor: STATE.channel === 'whatsapp' ? '#25D366' : '#FF9F43', backgroundColor: STATE.channel === 'whatsapp' ? 'rgba(37,211,102,0.1)' : 'rgba(255,159,67,0.1)', borderWidth: 2, fill: true, yAxisID: 'y', tension: 0.3 }},
            {{ label: 'OS ' + chLabel, data: osArr, borderColor: '#0467FC', backgroundColor: 'rgba(4,103,252,0.1)', borderWidth: 2, fill: true, yAxisID: 'y1', tension: 0.3 }}
        );
    }}

    // Add MD% line (independent of channel filter)
    const mdMap = aggregateMD(RAW.md_weekly);
    const mdArr = [];
    buckets.forEach(b => {{
        const md = mdMap.get(b);
        mdArr.push(md && md.count > 0 ? md.sum / md.count : null);
    }});
    datasets.push({{ label: 'MD%', data: mdArr, borderColor: '#A855F7', backgroundColor: 'transparent', borderWidth: 2, borderDash: [3,3], fill: false, yAxisID: 'y2', tension: 0.3, pointRadius: 3, pointBackgroundColor: '#A855F7' }});

    const config = {{
        type: 'line',
        data: {{ labels, datasets }},
        options: {{
            ...CHART_DEFAULTS,
            scales: {{
                x: CHART_DEFAULTS.scales.x,
                y: {{
                    ...CHART_DEFAULTS.scales.y,
                    position: 'left',
                    title: {{ display: true, text: 'Deliveries', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }},
                }},
                y1: {{
                    ...CHART_DEFAULTS.scales.y,
                    position: 'right',
                    grid: {{ drawOnChartArea: false, color: 'rgba(255,255,255,0.04)' }},
                    title: {{ display: true, text: 'OS', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }},
                }},
                y2: {{
                    ...CHART_DEFAULTS.scales.y,
                    position: 'right',
                    grid: {{ drawOnChartArea: false }},
                    ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }},
                    title: {{ display: true, text: 'MD%', color: '#A855F7', font: {{ size: 10 }} }},
                }},
            }}
        }}
    }};
    createChart('chart-volumes', config);
}}

// ── Rates Chart ──
function renderRatesChart(buckets, labels) {{
    const eng = aggregateEngagement(RAW.engagement_all);
    const osMap = aggregateOS(RAW.os_weekly);

    const orArr = [], ctrArr = [], cvrArr = [];
    buckets.forEach(b => {{
        const e = eng.get(b) || {{deliveries:0, opens:0, clicks:0}};
        const os = osMap.get(b) || 0;
        orArr.push(e.deliveries > 0 ? safePct(e.opens, e.deliveries) : null);
        ctrArr.push(e.deliveries > 0 ? safePct(e.clicks, e.deliveries) : null);
        cvrArr.push(e.deliveries > 0 ? safePct(os, e.deliveries) : null);
    }});

    const config = {{
        type: 'line',
        data: {{
            labels,
            datasets: [
                {{ label: 'OR%', data: orArr, borderColor: '#00C48C', borderWidth: 2, fill: false, tension: 0.3, pointRadius: 3, yAxisID: 'y' }},
                {{ label: 'CTR%', data: ctrArr, borderColor: '#FF9F43', borderWidth: 2, fill: false, tension: 0.3, pointRadius: 3, yAxisID: 'y1' }},
                {{ label: 'CVR%', data: cvrArr, borderColor: '#0467FC', borderWidth: 2, fill: false, tension: 0.3, pointRadius: 3, yAxisID: 'y1' }},
            ]
        }},
        options: {{
            ...CHART_DEFAULTS,
            scales: {{
                x: CHART_DEFAULTS.scales.x,
                y: {{ ...CHART_DEFAULTS.scales.y, position: 'left', title: {{ display: true, text: 'OR%', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }} }},
                y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'CTR% / CVR%', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }} }},
            }}
        }}
    }};
    createChart('chart-rates', config);
}}

// ── Campaign Evolution (Stacked Bar) ──
function renderCampaignEvo(buckets, labels) {{
    const osByType = aggregateOSByGroup(RAW.os_by_type, 'campaign_type_label');

    // Consistent colors for campaign types
    const TYPE_COLORS = {{
        'Pulse': '#0467FC',
        'Triggered': '#FF9F43',
        'Dormant Reac': '#A855F7',
        'Customers': '#00C48C',
        'Onboarding': '#38BDF8',
        'Email Cross': '#F472B6',
        'Promos': '#FBBF24',
        'Others': '#6B7280',
    }};

    // Get all types present in data
    const allTypes = [...osByType.keys()].sort((a, b) => {{
        // Sort by total OS descending
        let totalA = 0, totalB = 0;
        const mapA = osByType.get(a), mapB = osByType.get(b);
        if (mapA) mapA.forEach(v => totalA += v);
        if (mapB) mapB.forEach(v => totalB += v);
        return totalB - totalA;
    }});

    const datasets = allTypes.map(type => {{
        const osMap = osByType.get(type) || new Map();
        const data = buckets.map(b => Math.round(osMap.get(b) || 0));
        return {{
            label: type,
            data: data,
            backgroundColor: TYPE_COLORS[type] || '#6B7280',
            borderRadius: 2,
        }};
    }});

    const config = {{
        type: 'bar',
        data: {{ labels, datasets }},
        options: {{
            ...CHART_DEFAULTS,
            plugins: {{
                ...CHART_DEFAULTS.plugins,
                legend: {{ display: true, position: 'bottom', labels: {{ color: 'rgba(255,255,255,0.7)', boxWidth: 12, padding: 8, font: {{ size: 10 }} }} }},
            }},
            scales: {{
                x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'OS', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
            }},
        }}
    }};
    createChart('chart-campaign-evo', config);

    // Update tag
    const tag = document.getElementById('evo-table-tag');
    if (tag) {{
        tag.className = 'channel-tag ' + channelTagClass();
        tag.textContent = channelLabel();
    }}
}}

// ── Campaign Type Table ──
function renderCampaignTypeTable(buckets, labels) {{
    const engByType = aggregateEngByGroup(RAW.engagement_by_type, 'campaign_type');
    const osByType = aggregateOSByGroup(RAW.os_by_type, 'campaign_type_label');
    const delByCh = aggregateDeliveriesByChannel(RAW.engagement_all);

    // Merge group names
    const allGroups = new Set([...engByType.keys(), ...osByType.keys()]);

    // Identify last complete bucket
    let lastBucket = buckets[buckets.length - 1];
    let prevBucket = buckets.length >= 2 ? buckets[buckets.length - 2] : null;
    if (isWtd(lastBucket) && buckets.length >= 2) {{
        lastBucket = buckets[buckets.length - 2];
        prevBucket = buckets.length >= 3 ? buckets[buckets.length - 3] : null;
    }}

    // AVG range: up to 8 periods before lastBucket
    const lastIdx = buckets.indexOf(lastBucket);
    const avgStart = Math.max(0, lastIdx - 7);
    const avgBuckets = buckets.slice(avgStart, lastIdx);
    const _avgLabel = avgLabel();

    // Build rows
    const rows = [];
    let totalDel = 0, totalOpens = 0, totalClicks = 0, totalOS = 0, totalPromo = 0;

    allGroups.forEach(g => {{
        const engMap = engByType.get(g) || new Map();
        const osMapG = osByType.get(g) || new Map();

        const eLast = engMap.get(lastBucket) || {{deliveries:0, opens:0, clicks:0, promo_deliveries:0}};
        const osLast = osMapG.get(lastBucket) || 0;

        const ePrev = prevBucket ? (engMap.get(prevBucket) || {{deliveries:0, opens:0, clicks:0, promo_deliveries:0}}) : null;
        const osPrev = prevBucket ? (osMapG.get(prevBucket) || 0) : null;

        // AVG
        let avgDel = 0, avgOs = 0, avgCount = 0;
        avgBuckets.forEach(b => {{
            const e = engMap.get(b);
            const o = osMapG.get(b) || 0;
            if (e) {{ avgDel += e.deliveries; avgCount++; }}
            avgOs += o;
        }});
        avgDel = avgCount > 0 ? avgDel / avgCount : 0;
        avgOs = avgBuckets.length > 0 ? avgOs / avgBuckets.length : 0;

        const orPct = safePct(eLast.opens, eLast.deliveries);
        const ctrPct = safePct(eLast.clicks, eLast.deliveries);
        const cvrPct = safePct(osLast, eLast.deliveries);
        const osPer1k = eLast.deliveries > 0 ? (osLast / eLast.deliveries) * 1000 : 0;
        const promoPct = eLast.deliveries > 0 ? safePct(eLast.promo_deliveries || 0, eLast.deliveries) : 0;

        // CPOS for this type
        const dcLast = delByCh.get(lastBucket) || {{wa_del:0, email_del:0}};
        let totalChDel = 0, totalChCost = 0;
        if (STATE.channel === 'both') {{
            totalChDel = dcLast.wa_del + dcLast.email_del;
            totalChCost = dcLast.wa_del * RAW.wa_cost + dcLast.email_del * RAW.email_cost;
        }} else if (STATE.channel === 'whatsapp') {{
            totalChDel = dcLast.wa_del;
            totalChCost = dcLast.wa_del * RAW.wa_cost;
        }} else {{
            totalChDel = dcLast.email_del;
            totalChCost = dcLast.email_del * RAW.email_cost;
        }}
        const blendedCostPerDel = totalChDel > 0 ? totalChCost / totalChDel : RAW.wa_cost;
        const cpos = osLast > 0 ? (eLast.deliveries * blendedCostPerDel) / osLast : null;

        totalDel += eLast.deliveries;
        totalOpens += eLast.opens;
        totalClicks += eLast.clicks;
        totalOS += osLast;
        totalPromo += (eLast.promo_deliveries || 0);

        // Prev metrics for WoW
        const prevOrPct = ePrev && ePrev.deliveries > 0 ? safePct(ePrev.opens, ePrev.deliveries) : null;
        const prevCtrPct = ePrev && ePrev.deliveries > 0 ? safePct(ePrev.clicks, ePrev.deliveries) : null;
        const prevCvrPct = ePrev && ePrev.deliveries > 0 && osPrev !== null ? safePct(osPrev, ePrev.deliveries) : null;
        const prevOsPer1k = ePrev && ePrev.deliveries > 0 && osPrev !== null ? (osPrev / ePrev.deliveries) * 1000 : null;
        const prevCpos = osPrev > 0 && ePrev ? (ePrev.deliveries * blendedCostPerDel) / osPrev : null;

        rows.push({{
            name: g,
            deliveries: eLast.deliveries,
            opens: eLast.opens,
            orPct, clicks: eLast.clicks, ctrPct,
            os: osLast, cvrPct, osPer1k, cpos,
            pctOs: 0, promoPct,
            // WoW for each metric
            delWow: ePrev ? wowChange(eLast.deliveries, ePrev.deliveries) : null,
            orWow: prevOrPct !== null ? (orPct - prevOrPct) : null,  // pp delta
            ctrWow: prevCtrPct !== null ? (ctrPct - prevCtrPct) : null,  // pp delta
            osWow: osPrev !== null ? wowChange(osLast, osPrev) : null,
            cvrWow: prevCvrPct !== null ? (cvrPct - prevCvrPct) : null,  // pp delta
            effWow: prevOsPer1k !== null ? wowChange(osPer1k, prevOsPer1k) : null,
            cposWow: prevCpos !== null && cpos !== null ? wowChange(cpos, prevCpos) : null,
            vsAvg: wowChange(osLast, avgOs),
            spark: buckets.map(b => {{
                const e2 = engMap.get(b) || {{deliveries:0}};
                const o2 = osMapG.get(b) || 0;
                return e2.deliveries > 0 ? (o2 / e2.deliveries) * 1000 : null;
            }}),
        }});
    }});

    // Compute %OS
    rows.forEach(r => {{ r.pctOs = totalOS > 0 ? safePct(r.os, totalOS) : 0; }});

    // Sort by OS/1K Del descending
    rows.sort((a, b) => b.osPer1k - a.osPer1k);

    // Totals row
    const totOrPct = safePct(totalOpens, totalDel);
    const totCtrPct = safePct(totalClicks, totalDel);
    const totCvrPct = safePct(totalOS, totalDel);
    const totOsPer1k = totalDel > 0 ? (totalOS / totalDel) * 1000 : 0;
    const totPromoPct = totalDel > 0 ? safePct(totalPromo, totalDel) : 0;

    // Render header
    const thead = document.getElementById('type-thead');
    thead.innerHTML = `
        <th data-sort="name">Type</th>
        <th data-sort="deliveries" class="num">Deliveries</th>
        <th data-sort="promoPct" class="num" title="% of deliveries with incentive (inctrue in UTM)">% Promo</th>
        <th data-sort="opens" class="num">Opens</th>
        <th data-sort="orPct" class="num">OR%</th>
        <th data-sort="clicks" class="num">Clicks</th>
        <th data-sort="ctrPct" class="num">CTR%</th>
        <th data-sort="os" class="num">OS</th>
        <th data-sort="cvrPct" class="num">CVR%</th>
        <th data-sort="osPer1k" class="num">OS/1K Del</th>
        <th data-sort="cpos" class="num">CPOS</th>
        <th data-sort="pctOs" class="num">%OS</th>
        <th data-sort="osWow" class="num">${{periodLabel()}}</th>
        <th data-sort="vsAvg" class="num">vs ${{_avgLabel}}</th>
        <th class="num">Trend</th>
    `;

    // Add sort listeners
    thead.querySelectorAll('th').forEach(th => {{
        th.addEventListener('click', () => {{
            const key = th.dataset.sort;
            const isNum = th.classList.contains('num');
            const dir = th.classList.contains('sorted-asc') ? 'desc' : 'asc';
            thead.querySelectorAll('th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
            th.classList.add('sorted-' + dir);
            rows.sort((a, b) => {{
                let va = a[key], vb = b[key];
                if (va === null || va === undefined) va = -Infinity;
                if (vb === null || vb === undefined) vb = -Infinity;
                if (typeof va === 'string') return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                return dir === 'asc' ? va - vb : vb - va;
            }});
            fillTypeBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct);
        }});
    }});

    fillTypeBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct);
}}

function fillTypeBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct) {{
    const tbody = document.getElementById('type-tbody');
    let html = '';
    rows.forEach((r, idx) => {{
        const tip = typeTooltip(r.name);
        const tipAttr = tip ? ` title="${{tip}}" style="cursor:help"` : '';
        const promoBar = r.promoPct > 0 ? `<span style="display:inline-block;width:${{Math.min(r.promoPct, 100)}}%%;max-width:40px;height:3px;background:#FF9F43;border-radius:2px;vertical-align:middle;margin-left:3px"></span>` : '';
        html += `<tr>
            <td class="campaign-name"${{tipAttr}}>${{r.name}}</td>
            <td${{wowCellAttr(r.delWow, false, false)}}>${{fmtNum(r.deliveries)}}</td>
            <td class="num">${{r.promoPct > 0 ? fmtPct(r.promoPct) : '--'}}${{promoBar}}</td>
            <td class="num">${{fmtNum(r.opens)}}</td>
            <td${{wowCellAttr(r.orWow, false, true)}}>${{fmtPct(r.orPct)}}</td>
            <td class="num">${{fmtNum(r.clicks)}}</td>
            <td${{wowCellAttr(r.ctrWow, false, true)}}>${{fmtPct(r.ctrPct)}}</td>
            <td${{wowCellAttr(r.osWow, false, false)}}>${{Math.round(r.os)}}</td>
            <td${{wowCellAttr(r.cvrWow, false, true)}}>${{fmtPct(r.cvrPct)}}</td>
            <td${{wowCellAttr(r.effWow, false, false)}} style="font-weight:800;color:var(--kavak-blue);font-size:12px">${{fmtEff(r.osPer1k)}}</td>
            <td${{wowCellAttr(r.cposWow, true, false)}}>${{r.cpos !== null ? fmtMoney(r.cpos) : '--'}}</td>
            <td class="num">${{fmtPct(r.pctOs)}}</td>
            <td class="num">${{changePillHtml(r.osWow)}}</td>
            <td class="num">${{changePillHtml(r.vsAvg)}}</td>
            <td class="num"><canvas id="type-spark-${{idx}}" width="60" height="20"></canvas></td>
        </tr>`;
    }});
    // Totals
    html += `<tr class="totals-row">
        <td class="campaign-name">TOTAL</td>
        <td class="num">${{fmtNum(totalDel)}}</td>
        <td class="num">${{totPromoPct > 0 ? fmtPct(totPromoPct) : '--'}}</td>
        <td class="num">${{fmtNum(totalOpens)}}</td>
        <td class="num">${{fmtPct(totOrPct)}}</td>
        <td class="num">${{fmtNum(totalClicks)}}</td>
        <td class="num">${{fmtPct(totCtrPct)}}</td>
        <td class="num">${{Math.round(totalOS)}}</td>
        <td class="num">${{fmtPct(totCvrPct)}}</td>
        <td class="num efficiency">${{fmtEff(totOsPer1k)}}</td>
        <td class="num">--</td>
        <td class="num">100%</td>
        <td class="num"></td>
        <td class="num"></td>
        <td class="num"></td>
    </tr>`;
    tbody.innerHTML = html;

    // Render sparklines
    rows.forEach((r, idx) => {{
        const c = document.getElementById('type-spark-' + idx);
        if (c && r.spark) {{
            const vals = r.spark.filter(v => v !== null);
            if (vals.length > 1) createSparkline(c, vals, '#0467FC');
        }}
    }});
}}

// ── Campaign Detail Table ──
function renderCampaignDetailTable(buckets, labels) {{
    const engByDetail = aggregateEngByGroup(RAW.engagement_detail, 'campaign_detail');
    const osByDetail = aggregateOSByGroup(RAW.os_detail, 'campaign_detail');
    const delByCh = aggregateDeliveriesByChannel(RAW.engagement_all);

    const allGroups = new Set([...engByDetail.keys(), ...osByDetail.keys()]);

    let lastBucket = buckets[buckets.length - 1];
    let prevBucket = buckets.length >= 2 ? buckets[buckets.length - 2] : null;
    if (isWtd(lastBucket) && buckets.length >= 2) {{
        lastBucket = buckets[buckets.length - 2];
        prevBucket = buckets.length >= 3 ? buckets[buckets.length - 3] : null;
    }}

    const lastIdx = buckets.indexOf(lastBucket);
    const avgStart = Math.max(0, lastIdx - 7);
    const avgBuckets = buckets.slice(avgStart, lastIdx);
    const _avgLabel = avgLabel();

    const rows = [];
    let totalDel = 0, totalOpens = 0, totalClicks = 0, totalOS = 0, totalPromo = 0;

    // Blended cost per delivery
    const dcLast = delByCh.get(lastBucket) || {{wa_del:0, email_del:0}};
    let totalChDel = 0, totalChCost = 0;
    if (STATE.channel === 'both') {{
        totalChDel = dcLast.wa_del + dcLast.email_del;
        totalChCost = dcLast.wa_del * RAW.wa_cost + dcLast.email_del * RAW.email_cost;
    }} else if (STATE.channel === 'whatsapp') {{
        totalChDel = dcLast.wa_del;
        totalChCost = dcLast.wa_del * RAW.wa_cost;
    }} else {{
        totalChDel = dcLast.email_del;
        totalChCost = dcLast.email_del * RAW.email_cost;
    }}
    const blendedCostPerDel = totalChDel > 0 ? totalChCost / totalChDel : RAW.wa_cost;

    allGroups.forEach(g => {{
        const engMap = engByDetail.get(g) || new Map();
        const osMapG = osByDetail.get(g) || new Map();

        const eLast = engMap.get(lastBucket) || {{deliveries:0, opens:0, clicks:0, promo_deliveries:0}};
        const osLast = osMapG.get(lastBucket) || 0;

        const ePrev = prevBucket ? (engMap.get(prevBucket) || {{deliveries:0, opens:0, clicks:0, promo_deliveries:0}}) : null;
        const osPrev = prevBucket ? (osMapG.get(prevBucket) || 0) : null;

        let avgOs = 0;
        avgBuckets.forEach(b => {{ avgOs += (osMapG.get(b) || 0); }});
        avgOs = avgBuckets.length > 0 ? avgOs / avgBuckets.length : 0;

        const orPct = safePct(eLast.opens, eLast.deliveries);
        const ctrPct = safePct(eLast.clicks, eLast.deliveries);
        const cvrPct = safePct(osLast, eLast.deliveries);
        const osPer1k = eLast.deliveries > 0 ? (osLast / eLast.deliveries) * 1000 : 0;
        const cpos = osLast > 0 ? (eLast.deliveries * blendedCostPerDel) / osLast : null;
        const promoPct = eLast.deliveries > 0 ? safePct(eLast.promo_deliveries || 0, eLast.deliveries) : 0;

        totalDel += eLast.deliveries;
        totalOpens += eLast.opens;
        totalClicks += eLast.clicks;
        totalOS += osLast;
        totalPromo += (eLast.promo_deliveries || 0);

        // Prev metrics for WoW
        const prevOrPct = ePrev && ePrev.deliveries > 0 ? safePct(ePrev.opens, ePrev.deliveries) : null;
        const prevCtrPct = ePrev && ePrev.deliveries > 0 ? safePct(ePrev.clicks, ePrev.deliveries) : null;
        const prevCvrPct = ePrev && ePrev.deliveries > 0 && osPrev !== null ? safePct(osPrev, ePrev.deliveries) : null;
        const prevOsPer1k = ePrev && ePrev.deliveries > 0 && osPrev !== null ? (osPrev / ePrev.deliveries) * 1000 : null;
        const prevCpos = osPrev > 0 && ePrev ? (ePrev.deliveries * blendedCostPerDel) / osPrev : null;

        rows.push({{
            name: g,
            deliveries: eLast.deliveries,
            opens: eLast.opens,
            orPct, clicks: eLast.clicks, ctrPct,
            os: osLast, cvrPct, osPer1k, cpos,
            pctOs: 0, promoPct,
            delWow: ePrev ? wowChange(eLast.deliveries, ePrev.deliveries) : null,
            orWow: prevOrPct !== null ? (orPct - prevOrPct) : null,
            ctrWow: prevCtrPct !== null ? (ctrPct - prevCtrPct) : null,
            osWow: osPrev !== null ? wowChange(osLast, osPrev) : null,
            cvrWow: prevCvrPct !== null ? (cvrPct - prevCvrPct) : null,
            effWow: prevOsPer1k !== null ? wowChange(osPer1k, prevOsPer1k) : null,
            cposWow: prevCpos !== null && cpos !== null ? wowChange(cpos, prevCpos) : null,
            vsAvg: wowChange(osLast, avgOs),
            avgOs: avgOs,
        }});
    }});

    rows.forEach(r => {{ r.pctOs = totalOS > 0 ? safePct(r.os, totalOS) : 0; }});

    // Sort by OS/1K Del
    rows.sort((a, b) => b.osPer1k - a.osPer1k);

    // TOP/LOW badges (min 500 deliveries for significance)
    const significant = rows.filter(r => r.deliveries >= 500);
    if (significant.length >= 3) {{
        const effValues = significant.map(r => r.osPer1k).sort((a,b) => b - a);
        const topThreshold = effValues[Math.min(2, effValues.length - 1)];
        const lowThreshold = effValues[Math.max(effValues.length - 3, 0)];
        rows.forEach(r => {{
            r.badge = '';
            if (r.deliveries >= 500 && r.osPer1k >= topThreshold && r.osPer1k > 0) r.badge = 'top';
            else if (r.deliveries >= 500 && r.osPer1k <= lowThreshold && r.osPer1k < topThreshold) r.badge = 'low';
        }});
    }}

    const totOrPct = safePct(totalOpens, totalDel);
    const totCtrPct = safePct(totalClicks, totalDel);
    const totCvrPct = safePct(totalOS, totalDel);
    const totOsPer1k = totalDel > 0 ? (totalOS / totalDel) * 1000 : 0;
    const totPromoPct = totalDel > 0 ? safePct(totalPromo, totalDel) : 0;

    // Render header
    const thead = document.getElementById('detail-thead');
    thead.innerHTML = `
        <th data-sort="name">Campaign</th>
        <th data-sort="deliveries" class="num">Deliveries</th>
        <th data-sort="promoPct" class="num" title="% of deliveries with incentive (inctrue in UTM)">% Promo</th>
        <th data-sort="opens" class="num">Opens</th>
        <th data-sort="orPct" class="num">OR%</th>
        <th data-sort="clicks" class="num">Clicks</th>
        <th data-sort="ctrPct" class="num">CTR%</th>
        <th data-sort="os" class="num">OS</th>
        <th data-sort="cvrPct" class="num">CVR%</th>
        <th data-sort="osPer1k" class="num">OS/1K Del</th>
        <th data-sort="cpos" class="num">CPOS</th>
        <th data-sort="pctOs" class="num">%OS</th>
        <th data-sort="osWow" class="num">${{periodLabel()}}</th>
        <th data-sort="vsAvg" class="num">vs ${{_avgLabel}}</th>
    `;

    thead.querySelectorAll('th').forEach(th => {{
        th.addEventListener('click', () => {{
            const key = th.dataset.sort;
            const dir = th.classList.contains('sorted-asc') ? 'desc' : 'asc';
            thead.querySelectorAll('th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
            th.classList.add('sorted-' + dir);
            rows.sort((a, b) => {{
                let va = a[key], vb = b[key];
                if (va === null || va === undefined) va = -Infinity;
                if (vb === null || vb === undefined) vb = -Infinity;
                if (typeof va === 'string') return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                return dir === 'asc' ? va - vb : vb - va;
            }});
            fillDetailBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct);
        }});
    }});

    fillDetailBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct);
}}

function fillDetailBody(rows, totalDel, totalOpens, totalClicks, totalOS, totOrPct, totCtrPct, totCvrPct, totOsPer1k, totPromoPct) {{
    const tbody = document.getElementById('detail-tbody');
    let html = '';
    rows.forEach(r => {{
        let badgeHtml = '';
        if (r.badge === 'top') badgeHtml = '<span class="top-badge">TOP</span>';
        else if (r.badge === 'low') badgeHtml = '<span class="low-badge">LOW</span>';

        const detTip = typeTooltip(r.name);
        const detTipAttr = detTip ? ` title="${{detTip}}" style="cursor:help"` : '';
        const promoBar = r.promoPct > 0 ? `<span style="display:inline-block;width:${{Math.min(r.promoPct, 100)}}%%;max-width:40px;height:3px;background:#FF9F43;border-radius:2px;vertical-align:middle;margin-left:3px"></span>` : '';
        html += `<tr>
            <td class="campaign-name"${{detTipAttr}}>${{r.name}}${{badgeHtml}}</td>
            <td${{wowCellAttr(r.delWow, false, false)}}>${{fmtNum(r.deliveries)}}</td>
            <td class="num">${{r.promoPct > 0 ? fmtPct(r.promoPct) : '--'}}${{promoBar}}</td>
            <td class="num">${{fmtNum(r.opens)}}</td>
            <td${{wowCellAttr(r.orWow, false, true)}}>${{fmtPct(r.orPct)}}</td>
            <td class="num">${{fmtNum(r.clicks)}}</td>
            <td${{wowCellAttr(r.ctrWow, false, true)}}>${{fmtPct(r.ctrPct)}}</td>
            <td${{wowCellAttr(r.osWow, false, false)}}>${{Math.round(r.os)}}</td>
            <td${{wowCellAttr(r.cvrWow, false, true)}}>${{fmtPct(r.cvrPct)}}</td>
            <td${{wowCellAttr(r.effWow, false, false)}} style="font-weight:800;color:var(--kavak-blue);font-size:12px">${{fmtEff(r.osPer1k)}}</td>
            <td${{wowCellAttr(r.cposWow, true, false)}}>${{r.cpos !== null ? fmtMoney(r.cpos) : '--'}}</td>
            <td class="num">${{fmtPct(r.pctOs)}}</td>
            <td class="num">${{changePillHtml(r.osWow)}}</td>
            <td class="num">${{changePillHtml(r.vsAvg)}}</td>
        </tr>`;
    }});
    html += `<tr class="totals-row">
        <td class="campaign-name">TOTAL</td>
        <td class="num">${{fmtNum(totalDel)}}</td>
        <td class="num">${{totPromoPct > 0 ? fmtPct(totPromoPct) : '--'}}</td>
        <td class="num">${{fmtNum(totalOpens)}}</td>
        <td class="num">${{fmtPct(totOrPct)}}</td>
        <td class="num">${{fmtNum(totalClicks)}}</td>
        <td class="num">${{fmtPct(totCtrPct)}}</td>
        <td class="num">${{Math.round(totalOS)}}</td>
        <td class="num">${{fmtPct(totCvrPct)}}</td>
        <td class="num efficiency">${{fmtEff(totOsPer1k)}}</td>
        <td class="num">--</td>
        <td class="num">100%</td>
        <td class="num"></td>
        <td class="num"></td>
    </tr>`;
    tbody.innerHTML = html;
}}

// ── Value Prop Charts (Tab 1) ──
function renderVPCharts() {{
    const vp = aggValueProp();
    if (vp.length === 0) return;

    const vpLbls = vp.map(r => timeLabel(r.week));

    const VP_COLORS = {{
        'WoW': '#0467FC',
        'WoW+MD': '#38BDF8',
        'MD': '#A855F7',
        'Instant Offer': '#FF9F43',
        'Trade In': '#00C48C',
        'Generico': '#6B7280',
    }};

    // VP Mix chart (stacked bar, percentage shares)
    destroyChart('chart-vp-mix');
    createChart('chart-vp-mix', {{
        type: 'bar',
        data: {{
            labels: vpLbls,
            datasets: [
                {{ label: 'WoW', data: vp.map(r => r.pct_wow), backgroundColor: VP_COLORS['WoW'], borderRadius: 2 }},
                {{ label: 'WoW+MD', data: vp.map(r => r.pct_wowmd), backgroundColor: VP_COLORS['WoW+MD'], borderRadius: 2 }},
                {{ label: 'MD', data: vp.map(r => r.pct_md), backgroundColor: VP_COLORS['MD'], borderRadius: 2 }},
                {{ label: 'Instant Offer', data: vp.map(r => r.pct_offer), backgroundColor: VP_COLORS['Instant Offer'], borderRadius: 2 }},
                {{ label: 'Trade In', data: vp.map(r => r.pct_tradein), backgroundColor: VP_COLORS['Trade In'], borderRadius: 2 }},
                {{ label: 'Generico', data: vp.map(r => r.pct_generico), backgroundColor: VP_COLORS['Generico'], borderRadius: 2 }},
            ]
        }},
        options: {{
            ...CHART_DEFAULTS,
            plugins: {{
                ...CHART_DEFAULTS.plugins,
                legend: {{ display: true, position: 'bottom', labels: {{ color: 'rgba(255,255,255,0.7)', boxWidth: 12, padding: 8, font: {{ size: 10 }} }} }},
                tooltip: {{
                    ...CHART_DEFAULTS.plugins.tooltip,
                    callbacks: {{
                        label: function(ctx) {{
                            return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + '%';
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, max: 100,
                     ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }},
                     title: {{ display: true, text: '% Share', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
            }}
        }}
    }});

    // VP Repetition Rate chart (line)
    destroyChart('chart-vp-repetition');
    createChart('chart-vp-repetition', {{
        type: 'line',
        data: {{
            labels: vpLbls,
            datasets: [
                {{
                    label: '% Repeated VP',
                    data: vp.map(r => r.repetition_pct),
                    borderColor: '#FF4757',
                    backgroundColor: 'rgba(255,71,87,0.08)',
                    borderWidth: 2.5,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 4,
                }},
                {{
                    label: 'Repeated Users',
                    data: vp.map(r => r.repeated_vp_users),
                    borderColor: '#FF9F43',
                    borderWidth: 2,
                    borderDash: [5, 3],
                    fill: false,
                    tension: 0.3,
                    pointRadius: 3,
                    yAxisID: 'y1',
                }}
            ]
        }},
        options: {{
            ...CHART_DEFAULTS,
            scales: {{
                x: CHART_DEFAULTS.scales.x,
                y: {{ ...CHART_DEFAULTS.scales.y, position: 'left',
                     ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }},
                     title: {{ display: true, text: '% Repetition', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right',
                      grid: {{ drawOnChartArea: false }},
                      title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
            }}
        }}
    }});
}}

// ── Insights ──
function renderInsights(buckets) {{
    const insights = [];
    const pLabel = periodLabel();

    // Determine last complete bucket and previous bucket
    let lastIdx = buckets.length - 1;
    if (isWtd(buckets[lastIdx]) && buckets.length >= 2) lastIdx = buckets.length - 2;
    const lastBucket = buckets[lastIdx];
    const prevBucket = lastIdx > 0 ? buckets[lastIdx - 1] : null;

    // ── Aggregations ──
    const eng = aggregateEngagement(RAW.engagement_all);
    const osMap = aggregateOS(RAW.os_weekly);
    const mdMap = aggregateMD(RAW.md_weekly);
    const delByCh = aggregateDeliveriesByChannel(RAW.engagement_all);
    const engByType = aggregateEngByGroup(RAW.engagement_by_type, 'campaign_type');
    const osByType = aggregateOSByGroup(RAW.os_by_type, 'campaign_type');
    const engByDetail = aggregateEngByGroup(RAW.engagement_detail, 'campaign_detail');
    const osByDetail = aggregateOSByGroup(RAW.os_detail, 'campaign_detail');

    const eLast = eng.get(lastBucket) || {{deliveries:0, opens:0, clicks:0}};
    const ePrev = prevBucket ? (eng.get(prevBucket) || {{deliveries:0, opens:0, clicks:0}}) : null;
    const osLast = osMap.get(lastBucket) || 0;
    const osPrev = prevBucket ? (osMap.get(prevBucket) || 0) : null;
    const dcLast = delByCh.get(lastBucket) || {{wa_del:0, email_del:0}};
    const dcPrev = prevBucket ? (delByCh.get(prevBucket) || {{wa_del:0, email_del:0}}) : null;

    // Compute CPOS
    function calcCost(dc) {{
        let c = 0;
        if (STATE.channel === 'whatsapp' || STATE.channel === 'both') c += dc.wa_del * RAW.wa_cost;
        if (STATE.channel === 'email' || STATE.channel === 'both') c += dc.email_del * RAW.email_cost;
        return c;
    }}
    const costLast = calcCost(dcLast);
    const costPrev = dcPrev ? calcCost(dcPrev) : null;
    const cposLast = osLast > 0 ? costLast / osLast : null;
    const cposPrev = osPrev && osPrev > 0 && costPrev !== null ? costPrev / osPrev : null;

    // CTR
    const ctrLast = eLast.deliveries > 0 ? (eLast.clicks / eLast.deliveries) * 100 : null;
    const ctrPrev = ePrev && ePrev.deliveries > 0 ? (ePrev.clicks / ePrev.deliveries) * 100 : null;

    // MD
    const mdLast = mdMap.get(lastBucket);
    const mdPrev = prevBucket ? mdMap.get(prevBucket) : null;
    const mdValLast = mdLast && mdLast.count > 0 ? mdLast.sum / mdLast.count : null;
    const mdValPrev = mdPrev && mdPrev.count > 0 ? mdPrev.sum / mdPrev.count : null;

    // CVR (OS/Del)
    const cvrLast = eLast.deliveries > 0 ? (osLast / eLast.deliveries) * 100 : null;
    const cvrPrev = ePrev && ePrev.deliveries > 0 && osPrev !== null ? (osPrev / ePrev.deliveries) * 100 : null;

    // ── Helper: compute avg of last N periods for a value extractor ──
    function avgOfBuckets(bkts, extractFn) {{
        const vals = bkts.map(extractFn).filter(v => v !== null && v !== undefined && !isNaN(v));
        return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
    }}
    const avgStart = Math.max(0, lastIdx - 8);
    const histBuckets = buckets.slice(avgStart, lastIdx);  // up to 8 periods BEFORE current

    // ═══ INSIGHT 1: OS volume — what changed and why ═══
    if (osPrev !== null && osPrev > 0) {{
        const osWow = wowChange(osLast, osPrev);
        const avgOs = avgOfBuckets(histBuckets, b => osMap.get(b) || 0);
        const vsAvg = avgOs ? wowChange(osLast, avgOs) : null;
        const type = osWow >= 0 ? 'positive' : 'negative';
        const sign = osWow > 0 ? '+' : '';

        // Decompose: is it volume-driven or efficiency-driven?
        const delWow = ePrev ? wowChange(eLast.deliveries, ePrev.deliveries) : null;
        const effLast = eLast.deliveries > 0 ? (osLast / eLast.deliveries) * 1000 : 0;
        const effPrev = ePrev && ePrev.deliveries > 0 ? (osPrev / ePrev.deliveries) * 1000 : 0;
        const effWow = effPrev > 0 ? wowChange(effLast, effPrev) : null;

        let driver = '';
        if (delWow !== null && effWow !== null) {{
            if (Math.abs(delWow) > Math.abs(effWow) * 2) driver = ` — driven by send volume (${{delWow > 0 ? '+':''}}${{delWow.toFixed(0)}}% deliveries)`;
            else if (Math.abs(effWow) > Math.abs(delWow) * 2) driver = ` — driven by efficiency (${{effWow > 0 ? '+':''}}${{effWow.toFixed(0)}}% OS/1K Del)`;
            else if (Math.abs(delWow) > 3 && Math.abs(effWow) > 3) driver = ` — both volume (${{delWow > 0 ? '+':''}}${{delWow.toFixed(0)}}%) and efficiency (${{effWow > 0 ? '+':''}}${{effWow.toFixed(0)}}%)`;
        }}
        let vsAvgNote = '';
        if (vsAvg !== null && Math.abs(vsAvg) > 5) {{
            vsAvgNote = ` | vs avg: ${{vsAvg > 0 ? '+':''}}${{vsAvg.toFixed(0)}}%`;
        }}
        insights.push({{ type, text: `OS ${{sign}}${{osWow.toFixed(1)}}% ${{pLabel}} (${{fmtNum(osLast)}} vs ${{fmtNum(osPrev)}})${{driver}}${{vsAvgNote}}` }});
    }}

    // ═══ INSIGHT 2: Volume anomalies by campaign_type (exclude Triggered) ═══
    // Only show types whose deliveries or OS deviated significantly from their own avg
    if (prevBucket && histBuckets.length >= 3) {{
        const typeAnomalies = [];
        const allTypes = new Set([...engByType.keys(), ...osByType.keys()]);
        allTypes.forEach(t => {{
            if (t === 'Triggered') return;
            const engT = engByType.get(t) || new Map();
            const osT = osByType.get(t) || new Map();
            const delNow = (engT.get(lastBucket) || {{deliveries:0}}).deliveries;
            const osNow = osT.get(lastBucket) || 0;
            const avgDel = avgOfBuckets(histBuckets, b => (engT.get(b) || {{deliveries:0}}).deliveries);
            const avgOsT = avgOfBuckets(histBuckets, b => osT.get(b) || 0);

            const delDev = avgDel && avgDel > 100 ? ((delNow - avgDel) / avgDel) * 100 : null;
            const osDev = avgOsT && avgOsT > 5 ? ((osNow - avgOsT) / avgOsT) * 100 : null;

            // Only flag if > 25% deviation from avg
            if (delDev !== null && Math.abs(delDev) > 25) {{
                typeAnomalies.push({{ name: t, metric: 'deliveries', now: delNow, avg: avgDel, dev: delDev }});
            }}
            if (osDev !== null && Math.abs(osDev) > 25) {{
                typeAnomalies.push({{ name: t, metric: 'OS', now: osNow, avg: avgOsT, dev: osDev }});
            }}
        }});
        typeAnomalies.sort((a, b) => Math.abs(b.dev) - Math.abs(a.dev));
        typeAnomalies.slice(0, 3).forEach(a => {{
            const dir = a.dev > 0 ? 'above' : 'below';
            const type = a.metric === 'OS' ? (a.dev > 0 ? 'positive' : 'negative') : 'info';
            insights.push({{ type, text: `${{a.name}}: ${{fmtNum(Math.round(a.now))}} ${{a.metric}} (${{a.dev > 0 ? '+':''}}${{a.dev.toFixed(0)}}% vs avg ${{fmtNum(Math.round(a.avg))}})` }});
        }});
    }}

    // ═══ INSIGHT 3: Campaign-level anomalies — what specific campaigns drove the change ═══
    if (prevBucket && histBuckets.length >= 3) {{
        const detailAnomalies = [];
        const allDetails = new Set([...engByDetail.keys(), ...osByDetail.keys()]);
        allDetails.forEach(d => {{
            const engD = engByDetail.get(d) || new Map();
            const osD = osByDetail.get(d) || new Map();
            const delNow = (engD.get(lastBucket) || {{deliveries:0}}).deliveries;
            const avgDel = avgOfBuckets(histBuckets, b => (engD.get(b) || {{deliveries:0}}).deliveries);

            // Show campaigns that deviated > 40% from their avg AND have meaningful volume
            if (avgDel !== null && avgDel > 200) {{
                const delDev = ((delNow - avgDel) / avgDel) * 100;
                const absDelta = delNow - avgDel;
                if (Math.abs(delDev) > 40 && Math.abs(absDelta) > 1000) {{
                    const osNow = osD.get(lastBucket) || 0;
                    detailAnomalies.push({{ name: d, delNow, avgDel, delDev, absDelta, os: osNow }});
                }}
            }}
        }});
        detailAnomalies.sort((a, b) => Math.abs(b.absDelta) - Math.abs(a.absDelta));
        detailAnomalies.slice(0, 2).forEach(a => {{
            const sign = a.absDelta > 0 ? '+' : '';
            const type = a.absDelta > 0 ? 'info' : 'negative';
            const osNote = a.os > 0 ? ` → ${{Math.round(a.os)}} OS` : '';
            insights.push({{ type, text: `${{a.name}}: ${{fmtNum(a.delNow)}} deliveries (${{sign}}${{fmtNum(Math.round(a.absDelta))}} vs avg)${{osNote}}` }});
        }});
    }}

    // ═══ INSIGHT 4: MD effect on conversions ═══
    if (mdValLast !== null && mdValPrev !== null) {{
        const mdDelta = mdValLast - mdValPrev;
        if (Math.abs(mdDelta) >= 0.3) {{
            // MD down = good (inverted)
            const mdType = mdDelta < 0 ? 'positive' : 'negative';
            let mdText = `MD ${{mdDelta < 0 ? 'improved' : 'worsened'}}: ${{mdValLast.toFixed(1)}}% (${{mdDelta > 0 ? '+':''}}${{mdDelta.toFixed(1)}}pp ${{pLabel}})`;

            // Correlate with CVR and CPOS
            const effects = [];
            if (cvrLast !== null && cvrPrev !== null) {{
                const cvrDelta = cvrLast - cvrPrev;
                if (Math.abs(cvrDelta) >= 0.005) effects.push(`CVR ${{cvrDelta > 0 ? '+':''}}${{(cvrDelta).toFixed(3)}}pp`);
            }}
            if (cposLast !== null && cposPrev !== null) {{
                const cposChg = wowChange(cposLast, cposPrev);
                if (Math.abs(cposChg) >= 3) effects.push(`CPOS ${{cposChg > 0 ? '+':''}}${{cposChg.toFixed(0)}}%`);
            }}
            if (effects.length > 0) mdText += ` — effect: ${{effects.join(', ')}}`;
            insights.push({{ type: mdType, text: mdText }});
        }}
    }}

    // ═══ INSIGHT 5: CPOS/CTR anomalies (only if they deviated significantly) ═══
    {{
        const avgCpos = avgOfBuckets(histBuckets, b => {{
            const o = osMap.get(b) || 0;
            const dc = delByCh.get(b) || {{wa_del:0, email_del:0}};
            const c = calcCost(dc);
            return o > 0 ? c / o : null;
        }});
        const avgCtr = avgOfBuckets(histBuckets, b => {{
            const e = eng.get(b) || {{deliveries:0, clicks:0}};
            return e.deliveries > 0 ? (e.clicks / e.deliveries) * 100 : null;
        }});
        const flags = [];
        if (cposLast !== null && avgCpos !== null) {{
            const cposDev = wowChange(cposLast, avgCpos);
            if (Math.abs(cposDev) > 10) {{
                // CPOS: inverted (up = bad)
                flags.push({{ text: `CPOS ${{fmtMoney(cposLast)}} (${{cposDev > 0 ? '+':''}}${{cposDev.toFixed(0)}}% vs avg ${{fmtMoney(avgCpos)}})`, bad: cposDev > 0 }});
            }}
        }}
        if (ctrLast !== null && avgCtr !== null) {{
            const ctrDev = ctrLast - avgCtr;
            if (Math.abs(ctrDev) > 0.1) {{
                flags.push({{ text: `CTR ${{ctrLast.toFixed(2)}}% (${{ctrDev > 0 ? '+':''}}${{ctrDev.toFixed(2)}}pp vs avg ${{avgCtr.toFixed(2)}}%)`, bad: ctrDev < 0 }});
            }}
        }}
        flags.forEach(f => insights.push({{ type: f.bad ? 'negative' : 'positive', text: f.text }}));
    }}

    // ═══ INSIGHT 6: Next steps (contextual, based on what anomalies were detected) ═══
    {{
        const actions = [];
        if (cposLast !== null && cposPrev !== null && cposLast > cposPrev * 1.10) {{
            actions.push('CPOS up — review which campaign types are driving cost increase');
        }}
        if (osPrev !== null && osLast < osPrev * 0.90) {{
            actions.push('OS dropped significantly — check if a campaign was paused or if efficiency decreased');
        }}
        if (ctrLast !== null && ctrPrev !== null && ctrLast < ctrPrev * 0.85) {{
            actions.push('CTR declining — review creatives, subject lines, and messaging');
        }}
        if (mdValLast !== null && mdValPrev !== null && mdValLast > mdValPrev + 0.5 && cvrLast !== null && cvrPrev !== null && cvrLast <= cvrPrev) {{
            actions.push('MD worsened but CVR flat — demand may be absorbing worse deals, monitor closely');
        }}
        if (mdValLast !== null && mdValPrev !== null && mdValLast < mdValPrev - 0.5 && cvrLast !== null && cvrPrev !== null && cvrLast < cvrPrev) {{
            actions.push('MD improved but CVR dropped — efficiency may be declining despite better pricing');
        }}
        if (actions.length > 0) {{
            insights.push({{ type: 'info', text: `Next steps: ${{actions.join(' | ')}}` }});
        }}
    }}

    // ── Render ──
    const section = document.getElementById('insights-section');
    const grid = document.getElementById('insights-grid');
    if (insights.length === 0) {{ section.style.display = 'none'; return; }}
    section.style.display = '';
    grid.innerHTML = insights.map(i =>
        `<div class="insight ${{i.type}}">${{i.text}}</div>`
    ).join('');
}}

// ════════════════════════════════════════════════════════════════════════════
// MODAL (click-to-expand charts)
// ════════════════════════════════════════════════════════════════════════════

function openModal(chartType) {{
    currentModalType = chartType;
    const overlay = document.getElementById('modal-overlay');
    overlay.classList.add('show');

    // Destroy old modal chart
    if (modalChart) {{ modalChart.destroy(); modalChart = null; }}

    const canvas = document.getElementById('modal-canvas');
    const ctx = canvas.getContext('2d');

    const buckets = getSortedBuckets();
    const labels = buckets.map(b => timeLabel(b));

    let config;

    if (chartType === 'volumes') {{
        const datasets = [];
        if (STATE.channel === 'both') {{
            const engByCh = aggregateEngagementByChannel(RAW.engagement_all);
            const osByCh = aggregateOSByChannel(RAW.os_weekly);
            const waDelArr = [], emailDelArr = [], waOsArr = [], emailOsArr = [];
            buckets.forEach(b => {{
                const ch = engByCh.get(b);
                waDelArr.push(ch && ch.get('whatsapp') ? ch.get('whatsapp').deliveries : 0);
                emailDelArr.push(ch && ch.get('email') ? ch.get('email').deliveries : 0);
                const os = osByCh.get(b);
                waOsArr.push(os && os.get('whatsapp') ? os.get('whatsapp') : 0);
                emailOsArr.push(os && os.get('email') ? os.get('email') : 0);
            }});
            datasets.push(
                {{ label: 'Deliveries WA', data: waDelArr, borderColor: '#25D366', borderWidth: 2, fill: false, yAxisID: 'y', tension: 0.3 }},
                {{ label: 'Deliveries Email', data: emailDelArr, borderColor: '#FF9F43', borderWidth: 2, borderDash: [5,3], fill: false, yAxisID: 'y', tension: 0.3 }},
                {{ label: 'OS WA', data: waOsArr, borderColor: '#0467FC', borderWidth: 2, fill: false, yAxisID: 'y1', tension: 0.3 }},
                {{ label: 'OS Email', data: emailOsArr, borderColor: '#A855F7', borderWidth: 2, borderDash: [5,3], fill: false, yAxisID: 'y1', tension: 0.3 }}
            );
        }} else {{
            const eng = aggregateEngagement(RAW.engagement_all);
            const osMap = aggregateOS(RAW.os_weekly);
            const delArr = [], osArr = [];
            buckets.forEach(b => {{
                const e = eng.get(b) || {{deliveries:0}};
                delArr.push(e.deliveries);
                osArr.push(osMap.get(b) || 0);
            }});
            const chLabel = channelLabel();
            datasets.push(
                {{ label: 'Deliveries ' + chLabel, data: delArr, borderColor: STATE.channel === 'whatsapp' ? '#25D366' : '#FF9F43', borderWidth: 2.5, fill: true, backgroundColor: STATE.channel === 'whatsapp' ? 'rgba(37,211,102,0.08)' : 'rgba(255,159,67,0.08)', yAxisID: 'y', tension: 0.3 }},
                {{ label: 'OS ' + chLabel, data: osArr, borderColor: '#0467FC', borderWidth: 2.5, fill: true, backgroundColor: 'rgba(4,103,252,0.08)', yAxisID: 'y1', tension: 0.3 }}
            );
        }}
        // Add MD% line (independent of channel filter)
        const mdMap = aggregateMD(RAW.md_weekly);
        const mdArr = [];
        buckets.forEach(b => {{
            const md = mdMap.get(b);
            mdArr.push(md && md.count > 0 ? md.sum / md.count : null);
        }});
        datasets.push({{ label: 'MD%', data: mdArr, borderColor: '#A855F7', backgroundColor: 'transparent', borderWidth: 2, borderDash: [3,3], fill: false, yAxisID: 'y2', tension: 0.3, pointRadius: 3, pointBackgroundColor: '#A855F7' }});
        config = {{
            type: 'line',
            data: {{ labels, datasets }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: CHART_DEFAULTS.scales.x,
                    y: {{ ...CHART_DEFAULTS.scales.y, position: 'left', title: {{ display: true, text: 'Deliveries', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                    y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'OS', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                    y2: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }}, title: {{ display: true, text: 'MD%', color: '#A855F7', font: {{ size: 11 }} }} }},
                }},
            }}
        }};
    }} else if (chartType === 'rates') {{
        const eng = aggregateEngagement(RAW.engagement_all);
        const osMap = aggregateOS(RAW.os_weekly);
        const orArr = [], ctrArr = [], cvrArr = [];
        buckets.forEach(b => {{
            const e = eng.get(b) || {{deliveries:0, opens:0, clicks:0}};
            const os = osMap.get(b) || 0;
            orArr.push(e.deliveries > 0 ? safePct(e.opens, e.deliveries) : null);
            ctrArr.push(e.deliveries > 0 ? safePct(e.clicks, e.deliveries) : null);
            cvrArr.push(e.deliveries > 0 ? safePct(os, e.deliveries) : null);
        }});
        config = {{
            type: 'line',
            data: {{
                labels,
                datasets: [
                    {{ label: 'OR%', data: orArr, borderColor: '#00C48C', borderWidth: 2.5, fill: false, tension: 0.3, pointRadius: 4, yAxisID: 'y' }},
                    {{ label: 'CTR%', data: ctrArr, borderColor: '#FF9F43', borderWidth: 2.5, fill: false, tension: 0.3, pointRadius: 4, yAxisID: 'y1' }},
                    {{ label: 'CVR%', data: cvrArr, borderColor: '#0467FC', borderWidth: 2.5, fill: false, tension: 0.3, pointRadius: 4, yAxisID: 'y1' }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: CHART_DEFAULTS.scales.x,
                    y: {{ ...CHART_DEFAULTS.scales.y, position: 'left', title: {{ display: true, text: 'OR%', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }} }},
                    y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'CTR% / CVR%', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }} }},
                }}
            }}
        }};
    }} else if (chartType === 'campaignEvo') {{
        const osByType = aggregateOSByGroup(RAW.os_by_type, 'campaign_type_label');
        const TYPE_COLORS = {{
            'Pulse': '#0467FC', 'Triggered': '#FF9F43', 'Dormant Reac': '#A855F7',
            'Customers': '#00C48C', 'Onboarding': '#38BDF8', 'Email Cross': '#F472B6',
            'Promos': '#FBBF24', 'Others': '#6B7280',
        }};
        const allTypes = [...osByType.keys()].sort((a, b) => {{
            let totalA = 0, totalB = 0;
            const mapA = osByType.get(a), mapB = osByType.get(b);
            if (mapA) mapA.forEach(v => totalA += v);
            if (mapB) mapB.forEach(v => totalB += v);
            return totalB - totalA;
        }});
        const datasets = allTypes.map(type => {{
            const osMap = osByType.get(type) || new Map();
            return {{
                label: type,
                data: buckets.map(b => Math.round(osMap.get(b) || 0)),
                backgroundColor: TYPE_COLORS[type] || '#6B7280',
                borderRadius: 2,
            }};
        }});
        config = {{
            type: 'bar',
            data: {{ labels, datasets }},
            options: {{
                ...CHART_DEFAULTS,
                plugins: {{
                    ...CHART_DEFAULTS.plugins,
                    legend: {{ display: true, position: 'bottom', labels: {{ color: 'rgba(255,255,255,0.7)', boxWidth: 14, padding: 10, font: {{ size: 11 }} }} }},
                }},
                scales: {{
                    x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                    y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'OS', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                }},
            }}
        }};
    }} else if (chartType === 'freqTrend') {{
        const sat = aggSatFreq();
        const lbls = sat.map(r => timeLabel(r.week));
        config = {{
            type: 'line',
            data: {{ labels: lbls, datasets: [
                {{ label: 'Avg WA/Impacted User', data: sat.map(r => r.avg_sends_per_impacted), borderColor: '#FF9F43', borderWidth: 2.5, fill: false, tension: 0.3, yAxisID: 'y', pointRadius: 4 }},
                {{ label: 'Avg WA/Pulse User', data: sat.map(r => r.avg_sends_per_pulse_user), borderColor: '#0467FC', borderWidth: 2.5, fill: false, tension: 0.3, yAxisID: 'y', pointRadius: 4 }},
                {{ label: '% Impacted', data: sat.map(r => r.pct_impacted), borderColor: '#00C48C', borderWidth: 2.5, borderDash: [5,3], fill: false, tension: 0.3, yAxisID: 'y1', pointRadius: 4 }},
            ] }},
            options: {{ ...CHART_DEFAULTS, scales: {{
                x: CHART_DEFAULTS.scales.x,
                y: {{ ...CHART_DEFAULTS.scales.y, position: 'left', title: {{ display: true, text: 'Avg WA Sends', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }}, title: {{ display: true, text: '% Impacted', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
            }} }}
        }};
    }} else if (chartType === 'freqDist') {{
        const sat = aggSatFreq();
        const lbls = sat.map(r => timeLabel(r.week));
        config = {{
            type: 'bar',
            data: {{ labels: lbls, datasets: [
                {{ label: '1 WA', data: sat.map(r => r.freq_1), backgroundColor: '#00C48C', borderRadius: 2 }},
                {{ label: '2 WA', data: sat.map(r => r.freq_2), backgroundColor: '#0467FC', borderRadius: 2 }},
                {{ label: '3 WA', data: sat.map(r => r.freq_3), backgroundColor: '#FF9F43', borderRadius: 2 }},
                {{ label: '4+ WA', data: sat.map(r => r.freq_4plus), backgroundColor: '#FF4757', borderRadius: 2 }},
            ] }},
            options: {{ ...CHART_DEFAULTS, scales: {{
                x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
            }} }}
        }};
    }} else if (chartType === 'overlapStack') {{
        const ov = aggOverlap();
        const lbls = ov.map(r => timeLabel(r.week));
        config = {{
            type: 'bar',
            data: {{ labels: lbls, datasets: [
                {{ label: 'Supply Only', data: ov.map(r => r.supply_only), backgroundColor: '#0467FC', borderRadius: 2 }},
                {{ label: 'AE Only', data: ov.map(r => r.ae_only), backgroundColor: '#A855F7', borderRadius: 2 }},
                {{ label: 'Sales Only', data: ov.map(r => r.sales_only), backgroundColor: '#38BDF8', borderRadius: 2 }},
                {{ label: 'Supply + AE', data: ov.map(r => r.supply_ae_only), backgroundColor: '#FF4757', borderRadius: 2 }},
                {{ label: 'Supply + Sales', data: ov.map(r => r.supply_sales_only), backgroundColor: '#FF9F43', borderRadius: 2 }},
                {{ label: 'AE + Sales', data: ov.map(r => r.ae_sales_only), backgroundColor: '#F472B6', borderRadius: 2 }},
                {{ label: 'All 3', data: ov.map(r => r.all_three), backgroundColor: '#FBBF24', borderRadius: 2 }},
            ] }},
            options: {{ ...CHART_DEFAULTS, scales: {{
                x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
            }} }}
        }};
    }} else if (chartType === 'overlapPct') {{
        const ov = aggOverlap();
        const lbls = ov.map(r => timeLabel(r.week));
        config = {{
            type: 'line',
            data: {{ labels: lbls, datasets: [
                {{ label: '% Multi-Vertical', data: ov.map(r => r.users_with_any_wa > 0 ? (r.multi_vertical_users / r.users_with_any_wa * 100) : 0), borderColor: '#FF4757', borderWidth: 2.5, fill: true, backgroundColor: 'rgba(255,71,87,0.1)', tension: 0.3, pointRadius: 4 }},
                {{ label: '% Supply+AE', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.supply_ae_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#A855F7', borderWidth: 2, fill: false, tension: 0.3, pointRadius: 4 }},
                {{ label: '% Supply+Sales', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.supply_sales_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#FF9F43', borderWidth: 2, borderDash: [5,3], fill: false, tension: 0.3, pointRadius: 4 }},
                {{ label: '% AE+Sales', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.ae_sales_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#F472B6', borderWidth: 2, borderDash: [3,3], fill: false, tension: 0.3, pointRadius: 4 }},
            ] }},
            options: {{ ...CHART_DEFAULTS, scales: {{
                x: CHART_DEFAULTS.scales.x,
                y: {{ ...CHART_DEFAULTS.scales.y, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }}, title: {{ display: true, text: '% of WA Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
            }} }}
        }};
    }} else if (chartType === 'vpMix') {{
        const vp = aggValueProp();
        const vpLbls = vp.map(r => timeLabel(r.week));
        config = {{
            type: 'bar',
            data: {{
                labels: vpLbls,
                datasets: [
                    {{ label: 'WoW', data: vp.map(r => r.pct_wow), backgroundColor: '#0467FC', borderRadius: 2 }},
                    {{ label: 'WoW+MD', data: vp.map(r => r.pct_wowmd), backgroundColor: '#38BDF8', borderRadius: 2 }},
                    {{ label: 'MD', data: vp.map(r => r.pct_md), backgroundColor: '#A855F7', borderRadius: 2 }},
                    {{ label: 'Instant Offer', data: vp.map(r => r.pct_offer), backgroundColor: '#FF9F43', borderRadius: 2 }},
                    {{ label: 'Trade In', data: vp.map(r => r.pct_tradein), backgroundColor: '#00C48C', borderRadius: 2 }},
                    {{ label: 'Generico', data: vp.map(r => r.pct_generico), backgroundColor: '#6B7280', borderRadius: 2 }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                plugins: {{ ...CHART_DEFAULTS.plugins, legend: {{ display: true, position: 'bottom', labels: {{ color: 'rgba(255,255,255,0.7)', boxWidth: 14, padding: 10, font: {{ size: 11 }} }} }} }},
                scales: {{
                    x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                    y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, max: 100,
                         ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }},
                         title: {{ display: true, text: '% Share', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                }}
            }}
        }};
    }} else if (chartType === 'vpRepetition') {{
        const vp = aggValueProp();
        const vpLbls = vp.map(r => timeLabel(r.week));
        config = {{
            type: 'line',
            data: {{
                labels: vpLbls,
                datasets: [
                    {{ label: '% Repeated VP', data: vp.map(r => r.repetition_pct), borderColor: '#FF4757', backgroundColor: 'rgba(255,71,87,0.1)', borderWidth: 2.5, fill: true, tension: 0.3, pointRadius: 4 }},
                    {{ label: 'Repeated Users', data: vp.map(r => r.repeated_vp_users), borderColor: '#FF9F43', borderWidth: 2, borderDash: [5,3], fill: false, tension: 0.3, pointRadius: 3, yAxisID: 'y1' }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: CHART_DEFAULTS.scales.x,
                    y: {{ ...CHART_DEFAULTS.scales.y, position: 'left',
                         ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }},
                         title: {{ display: true, text: '% Repetition', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                    y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }},
                          title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 11 }} }} }},
                }}
            }}
        }};
    }}

    // ── Combiner: custom multi-metric chart — each metric gets its own Y-axis ──
    if (chartType === 'combiner' && combinerSelections.size >= 2) {{
        const selected = METRIC_REGISTRY.filter(m => combinerSelections.has(m.id));
        const datasets = [];
        const scales = {{ x: CHART_DEFAULTS.scales.x }};

        // Each metric gets its own Y-axis for independent scaling
        selected.forEach((m, i) => {{
            const data = m.extract(buckets);
            const axisId = 'y_' + i;
            const isLeft = i === 0;  // First metric on left, rest on right

            datasets.push({{
                label: m.label,
                data: data,
                borderColor: m.color,
                backgroundColor: m.color + '22',
                borderWidth: 2.5,
                fill: false,
                tension: 0.3,
                pointRadius: 4,
                pointHoverRadius: 6,
                yAxisID: axisId,
            }});

            const tickCb = m.unit === 'pct' ? yAxisCallbackPct
                         : m.unit === 'money' ? (v => '$' + yAxisCallback(v))
                         : yAxisCallback;

            scales[axisId] = {{
                type: 'linear',
                position: isLeft ? 'left' : 'right',
                display: i < 4,  // Show max 4 axes (hide rest but keep scaling)
                grid: {{ drawOnChartArea: isLeft, color: isLeft ? 'rgba(255,255,255,0.04)' : 'transparent' }},
                ticks: {{
                    color: m.color + 'CC',
                    font: {{ family: 'Inter', size: 10 }},
                    callback: tickCb,
                    maxTicksLimit: 6,
                }},
                title: {{
                    display: i < 4,
                    text: m.label,
                    color: m.color + 'AA',
                    font: {{ size: 10 }},
                }},
                beginAtZero: m.unit !== 'pct',
            }};
        }});

        config = {{
            type: 'line',
            data: {{ labels, datasets }},
            options: {{
                ...CHART_DEFAULTS,
                scales,
                plugins: {{
                    ...CHART_DEFAULTS.plugins,
                    tooltip: {{
                        ...CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {{
                            label: function(ctx) {{
                                const m = selected[ctx.datasetIndex];
                                const val = ctx.parsed.y;
                                if (val === null || val === undefined) return m.label + ': --';
                                if (m.unit === 'pct') return m.label + ': ' + val.toFixed(2) + '%';
                                if (m.unit === 'money') return m.label + ': $' + val.toFixed(2);
                                return m.label + ': ' + fmtNum(val);
                            }}
                        }}
                    }}
                }}
            }}
        }};
    }}

    if (config) {{
        modalChart = new Chart(ctx, config);
    }}
}}

function closeModal(event) {{
    if (event.target === document.getElementById('modal-overlay')) {{
        closeModalForce();
    }}
}}

function closeModalForce() {{
    document.getElementById('modal-overlay').classList.remove('show');
    if (modalChart) {{ modalChart.destroy(); modalChart = null; }}
    currentModalType = null;
}}

// Close on Escape
document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') closeModalForce();
}});

// ════════════════════════════════════════════════════════════════════════════
// SATURATION TAB — STATE + AGGREGATION
// ════════════════════════════════════════════════════════════════════════════

const SAT_STATE = {{ pulseType: 'all' }};
const PULSE_TYPE_LABELS = {{
    'pulse': 'Active',
    'pulse-dormants': 'Dormants',
    'pulse-dormants-2022': 'Dormants 2022',
    'pulse-past-sales': 'Past Sales',
}};

function setSatFilter(value) {{
    SAT_STATE.pulseType = value;
    renderSaturationTab();
}}

function buildSatFilterPills() {{
    const types = new Set();
    (RAW.saturation_frequency || []).forEach(r => {{ if (r.pulse_type) types.add(r.pulse_type); }});
    const container = document.getElementById('sat-filter-pills');
    if (!container) return;
    const cur = SAT_STATE.pulseType;
    let html = `<button class="pill ${{cur === 'all' ? 'active' : ''}}" data-filter="pulseType" onclick="setSatFilter('all')">All Types</button>`;
    [...types].sort().forEach(t => {{
        const label = PULSE_TYPE_LABELS[t] || t;
        const cls = cur === t ? 'active' : '';
        html += ` <button class="pill ${{cls}}" data-filter="pulseType" onclick="setSatFilter('${{t}}')">${{label}}</button>`;
    }});
    container.innerHTML = html;
}}

// Aggregate saturation_frequency by selected pulse_type
function aggSatFreq() {{
    const data = RAW.saturation_frequency || [];
    const f = SAT_STATE.pulseType;
    const map = new Map();
    data.forEach(r => {{
        if (f !== 'all' && r.pulse_type !== f) return;
        const w = r.week;
        if (!map.has(w)) map.set(w, {{week:w, total_pulse_users:0, users_with_wa:0, total_wa_sends:0, freq_1:0, freq_2:0, freq_3:0, freq_4plus:0}});
        const m = map.get(w);
        m.total_pulse_users += N(r.total_pulse_users);
        m.users_with_wa += N(r.users_with_wa);
        m.total_wa_sends += N(r.total_wa_sends);
        m.freq_1 += N(r.freq_1);
        m.freq_2 += N(r.freq_2);
        m.freq_3 += N(r.freq_3);
        m.freq_4plus += N(r.freq_4plus);
    }});
    const result = [...map.values()].sort((a,b) => a.week < b.week ? -1 : 1);
    result.forEach(r => {{
        r.pct_impacted = r.total_pulse_users > 0 ? (r.users_with_wa / r.total_pulse_users * 100) : 0;
        r.avg_sends_per_impacted = r.users_with_wa > 0 ? (r.total_wa_sends / r.users_with_wa) : 0;
        r.avg_sends_per_pulse_user = r.total_pulse_users > 0 ? (r.total_wa_sends / r.total_pulse_users) : 0;
    }});
    return result;
}}

// Aggregate vertical_overlap by selected pulse_type (now with exclusive categories)
function aggOverlap() {{
    const data = RAW.vertical_overlap || [];
    const f = SAT_STATE.pulseType;
    const map = new Map();
    data.forEach(r => {{
        if (f !== 'all' && r.pulse_type !== f) return;
        const w = r.week;
        if (!map.has(w)) map.set(w, {{week:w, users_with_any_wa:0, supply_only:0, ae_only:0, sales_only:0, supply_ae_only:0, supply_sales_only:0, ae_sales_only:0, all_three:0, multi_vertical_users:0}});
        const m = map.get(w);
        m.users_with_any_wa += N(r.users_with_any_wa);
        m.supply_only += N(r.supply_only);
        m.ae_only += N(r.ae_only);
        m.sales_only += N(r.sales_only);
        m.supply_ae_only += N(r.supply_ae_only);
        m.supply_sales_only += N(r.supply_sales_only);
        m.ae_sales_only += N(r.ae_sales_only);
        m.all_three += N(r.all_three);
        m.multi_vertical_users += N(r.multi_vertical_users);
    }});
    return [...map.values()].sort((a,b) => a.week < b.week ? -1 : 1);
}}

// Aggregate value_prop_repetition by selected pulse_type
function aggValueProp() {{
    const data = RAW.value_prop_repetition || [];
    const f = SAT_STATE.pulseType;
    const map = new Map();
    data.forEach(r => {{
        if (f !== 'all' && r.pulse_type !== f) return;
        const w = r.week;
        if (!map.has(w)) map.set(w, {{
            week: w, users_with_wa: 0, repeated_vp_users: 0,
            vp_wow: 0, vp_wowmd: 0, vp_md: 0, vp_offer: 0, vp_tradein: 0, vp_generico: 0
        }});
        const m = map.get(w);
        m.users_with_wa += N(r.users_with_wa);
        m.repeated_vp_users += N(r.repeated_vp_users);
        m.vp_wow += N(r.vp_wow);
        m.vp_wowmd += N(r.vp_wowmd);
        m.vp_md += N(r.vp_md);
        m.vp_offer += N(r.vp_offer);
        m.vp_tradein += N(r.vp_tradein);
        m.vp_generico += N(r.vp_generico);
    }});
    const result = [...map.values()].sort((a, b) => a.week < b.week ? -1 : 1);
    result.forEach(r => {{
        r.repetition_pct = r.users_with_wa > 0 ? (r.repeated_vp_users / r.users_with_wa * 100) : 0;
        const total = r.vp_wow + r.vp_wowmd + r.vp_md + r.vp_offer + r.vp_tradein + r.vp_generico;
        r.pct_wow = total > 0 ? (r.vp_wow / total * 100) : 0;
        r.pct_wowmd = total > 0 ? (r.vp_wowmd / total * 100) : 0;
        r.pct_md = total > 0 ? (r.vp_md / total * 100) : 0;
        r.pct_offer = total > 0 ? (r.vp_offer / total * 100) : 0;
        r.pct_tradein = total > 0 ? (r.vp_tradein / total * 100) : 0;
        r.pct_generico = total > 0 ? (r.vp_generico / total * 100) : 0;
    }});
    return result;
}}

// ════════════════════════════════════════════════════════════════════════════
// SATURATION TAB RENDERING
// ════════════════════════════════════════════════════════════════════════════

function renderSaturationTab() {{
    buildSatFilterPills();

    const sat = aggSatFreq();
    const ov = aggOverlap();

    if (sat.length === 0 && ov.length === 0) return;

    // ── KPIs ──
    const last = sat.length > 0 ? sat[sat.length - 1] : null;
    const prev = sat.length > 1 ? sat[sat.length - 2] : null;
    const lastO = ov.length > 0 ? ov[ov.length - 1] : null;
    const prevO = ov.length > 1 ? ov[ov.length - 2] : null;

    const kpiGrid = document.getElementById('sat-kpi-grid');
    if (kpiGrid && last) {{
        const pctI = last.pct_impacted;
        const prevPctI = prev ? prev.pct_impacted : null;
        const avgI = last.avg_sends_per_impacted;
        const prevAvgI = prev ? prev.avg_sends_per_impacted : null;
        const multiV = lastO ? (lastO.multi_vertical_users || 0) : 0;
        const prevMultiV = prevO ? (prevO.multi_vertical_users || 0) : null;
        const multiPct = lastO && lastO.users_with_any_wa > 0 ? (multiV / lastO.users_with_any_wa * 100) : 0;
        const prevMultiPct = prevO && prevO.users_with_any_wa > 0 && prevMultiV !== null ? (prevMultiV / prevO.users_with_any_wa * 100) : null;

        function kCard(label, valueFmt, change, inverted) {{
            let chg = '';
            if (change !== null && change !== undefined) {{
                const cls = inverted ? (change > 0 ? 'negative' : change < 0 ? 'positive' : 'neutral')
                                     : (change > 0 ? 'positive' : change < 0 ? 'negative' : 'neutral');
                const sign = change > 0 ? '+' : '';
                chg = `<span class="kpi-badge ${{cls}}">${{sign}}${{change.toFixed(1)}} WoW</span>`;
            }}
            return `<div class="kpi-card"><div class="kpi-label">${{label}}</div><div class="kpi-value">${{valueFmt}}</div><div class="kpi-meta">${{chg}}</div></div>`;
        }}
        kpiGrid.innerHTML = [
            kCard('Pulse Audience', fmtNum(last.total_pulse_users), null),
            kCard('% Impacted (WA)', pctI.toFixed(1) + '%', prevPctI !== null ? (pctI - prevPctI) : null, true),
            kCard('Avg WA / Impacted', avgI.toFixed(2) + 'x', prevAvgI !== null ? (avgI - prevAvgI) : null, true),
            kCard('Users w/ WA', fmtNum(last.users_with_wa), null),
            kCard('% Multi-Vertical', multiPct.toFixed(1) + '%', prevMultiPct !== null ? (multiPct - prevMultiPct) : null, true),
        ].join('');
    }}

    // ── Frequency Trend ──
    if (sat.length > 0) {{
        const lbls = sat.map(r => timeLabel(r.week));
        destroyChart('chart-freq-trend');
        createChart('chart-freq-trend', {{
            type: 'line',
            data: {{
                labels: lbls,
                datasets: [
                    {{ label: 'Avg WA/Impacted User', data: sat.map(r => r.avg_sends_per_impacted), borderColor: '#FF9F43', borderWidth: 2, fill: false, tension: 0.3, yAxisID: 'y', pointRadius: 3 }},
                    {{ label: 'Avg WA/Pulse User', data: sat.map(r => r.avg_sends_per_pulse_user), borderColor: '#0467FC', borderWidth: 2, fill: false, tension: 0.3, yAxisID: 'y', pointRadius: 3 }},
                    {{ label: '% Impacted', data: sat.map(r => r.pct_impacted), borderColor: '#00C48C', borderWidth: 2, borderDash: [5,3], fill: false, tension: 0.3, yAxisID: 'y1', pointRadius: 3 }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: CHART_DEFAULTS.scales.x,
                    y: {{ ...CHART_DEFAULTS.scales.y, position: 'left', title: {{ display: true, text: 'Avg Sends', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                    y1: {{ ...CHART_DEFAULTS.scales.y, position: 'right', grid: {{ drawOnChartArea: false }}, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }}, title: {{ display: true, text: '% Impacted', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                }}
            }}
        }});
    }}

    // ── Frequency Distribution ──
    if (sat.length > 0) {{
        const lbls = sat.map(r => timeLabel(r.week));
        destroyChart('chart-freq-dist');
        createChart('chart-freq-dist', {{
            type: 'bar',
            data: {{
                labels: lbls,
                datasets: [
                    {{ label: '1 WA', data: sat.map(r => r.freq_1), backgroundColor: '#00C48C', borderRadius: 2 }},
                    {{ label: '2 WA', data: sat.map(r => r.freq_2), backgroundColor: '#0467FC', borderRadius: 2 }},
                    {{ label: '3 WA', data: sat.map(r => r.freq_3), backgroundColor: '#FF9F43', borderRadius: 2 }},
                    {{ label: '4+ WA', data: sat.map(r => r.freq_4plus), backgroundColor: '#FF4757', borderRadius: 2 }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                    y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                }}
            }}
        }});
    }}

    // ── Vertical Overlap Stacked Bar (exclusive categories) ──
    if (ov.length > 0) {{
        const lbls = ov.map(r => timeLabel(r.week));
        destroyChart('chart-overlap-stack');
        createChart('chart-overlap-stack', {{
            type: 'bar',
            data: {{
                labels: lbls,
                datasets: [
                    {{ label: 'Supply Only', data: ov.map(r => r.supply_only), backgroundColor: '#0467FC', borderRadius: 2 }},
                    {{ label: 'AE Only', data: ov.map(r => r.ae_only), backgroundColor: '#A855F7', borderRadius: 2 }},
                    {{ label: 'Sales Only', data: ov.map(r => r.sales_only), backgroundColor: '#38BDF8', borderRadius: 2 }},
                    {{ label: 'Supply + AE', data: ov.map(r => r.supply_ae_only), backgroundColor: '#FF4757', borderRadius: 2 }},
                    {{ label: 'Supply + Sales', data: ov.map(r => r.supply_sales_only), backgroundColor: '#FF9F43', borderRadius: 2 }},
                    {{ label: 'AE + Sales', data: ov.map(r => r.ae_sales_only), backgroundColor: '#F472B6', borderRadius: 2 }},
                    {{ label: 'All 3', data: ov.map(r => r.all_three), backgroundColor: '#FBBF24', borderRadius: 2 }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: {{ ...CHART_DEFAULTS.scales.x, stacked: true }},
                    y: {{ ...CHART_DEFAULTS.scales.y, stacked: true, title: {{ display: true, text: 'Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                }}
            }}
        }});
    }}

    // ── Overlap % Trend (all combos) ──
    if (ov.length > 0) {{
        const lbls = ov.map(r => timeLabel(r.week));
        destroyChart('chart-overlap-pct');
        createChart('chart-overlap-pct', {{
            type: 'line',
            data: {{
                labels: lbls,
                datasets: [
                    {{ label: '% Multi-Vertical', data: ov.map(r => r.users_with_any_wa > 0 ? (r.multi_vertical_users / r.users_with_any_wa * 100) : 0), borderColor: '#FF4757', borderWidth: 2.5, fill: true, backgroundColor: 'rgba(255,71,87,0.1)', tension: 0.3, pointRadius: 3 }},
                    {{ label: '% Supply+AE', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.supply_ae_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#A855F7', borderWidth: 2, fill: false, tension: 0.3, pointRadius: 3 }},
                    {{ label: '% Supply+Sales', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.supply_sales_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#FF9F43', borderWidth: 2, borderDash: [5,3], fill: false, tension: 0.3, pointRadius: 3 }},
                    {{ label: '% AE+Sales', data: ov.map(r => r.users_with_any_wa > 0 ? ((r.ae_sales_only + r.all_three) / r.users_with_any_wa * 100) : 0), borderColor: '#F472B6', borderWidth: 2, borderDash: [3,3], fill: false, tension: 0.3, pointRadius: 3 }},
                ]
            }},
            options: {{
                ...CHART_DEFAULTS,
                scales: {{
                    x: CHART_DEFAULTS.scales.x,
                    y: {{ ...CHART_DEFAULTS.scales.y, ticks: {{ ...CHART_DEFAULTS.scales.y.ticks, callback: yAxisCallbackPct }}, title: {{ display: true, text: '% of WA Users', color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                }}
            }}
        }});
    }}

    // ── Table ──
    renderSaturationTable(sat, ov);
}}

function renderSaturationTable(sat, ov) {{
    const thead = document.getElementById('sat-thead');
    const tbody = document.getElementById('sat-tbody');
    if (!thead || !tbody) return;

    // Merge sat + ov by week
    const weekMap = new Map();
    sat.forEach(r => {{ weekMap.set(r.week, {{ ...r }}); }});
    ov.forEach(r => {{
        if (weekMap.has(r.week)) {{
            const m = weekMap.get(r.week);
            m.supply_only = r.supply_only; m.ae_only = r.ae_only; m.sales_only = r.sales_only;
            m.supply_ae_only = r.supply_ae_only; m.supply_sales_only = r.supply_sales_only;
            m.ae_sales_only = r.ae_sales_only; m.all_three = r.all_three;
            m.multi_vertical_users = r.multi_vertical_users; m.users_with_any_wa_ov = r.users_with_any_wa;
        }}
    }});

    const weeks = [...weekMap.keys()].sort().slice(-12);
    const cols = ['Week', 'Pulse', 'WA Users', '% Imp', 'Total WA', 'Avg/Imp', '1 WA', '2 WA', '3 WA', '4+',
                  'Supply', 'AE', 'S+AE', 'S+Sales', 'All 3', 'Multi-V %'];
    thead.innerHTML = cols.map(c => `<th>${{c}}</th>`).join('');

    const rows = weeks.map(w => {{
        const r = weekMap.get(w) || {{}};
        const pctI = r.total_pulse_users > 0 ? (r.users_with_wa / r.total_pulse_users * 100) : 0;
        const waU = r.users_with_any_wa_ov || r.users_with_wa || 0;
        const mvPct = waU > 0 ? ((r.multi_vertical_users || 0) / waU * 100) : 0;
        const avgI = r.users_with_wa > 0 ? (r.total_wa_sends / r.users_with_wa) : 0;

        return `<tr>
            <td style="font-weight:600">${{timeLabel(w)}}</td>
            <td>${{fmtNum(r.total_pulse_users || 0)}}</td>
            <td>${{fmtNum(r.users_with_wa || 0)}}</td>
            <td>${{pctI.toFixed(1)}}%</td>
            <td>${{fmtNum(r.total_wa_sends || 0)}}</td>
            <td style="font-weight:600">${{avgI.toFixed(2)}}x</td>
            <td>${{fmtNum(r.freq_1 || 0)}}</td>
            <td>${{fmtNum(r.freq_2 || 0)}}</td>
            <td>${{fmtNum(r.freq_3 || 0)}}</td>
            <td style="color:${{(r.freq_4plus || 0) > 0 ? 'var(--kavak-red)' : 'inherit'}}">${{fmtNum(r.freq_4plus || 0)}}</td>
            <td>${{fmtNum(r.supply_only || 0)}}</td>
            <td>${{fmtNum(r.ae_only || 0)}}</td>
            <td style="color:var(--kavak-red);font-weight:600">${{fmtNum(r.supply_ae_only || 0)}}</td>
            <td style="color:var(--kavak-orange)">${{fmtNum(r.supply_sales_only || 0)}}</td>
            <td style="color:var(--kavak-purple)">${{fmtNum(r.all_three || 0)}}</td>
            <td style="color:${{mvPct > 30 ? 'var(--kavak-red)' : mvPct > 15 ? 'var(--kavak-orange)' : 'inherit'}};font-weight:600">${{mvPct.toFixed(1)}}%</td>
        </tr>`;
    }}).reverse();

    tbody.innerHTML = rows.join('');
}}

// ════════════════════════════════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════════════════════════════════
renderAll();
</script>
</body>
</html>"""

    return html


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60, flush=True)
    print("Supply CRM Dashboard v2 — Generating...", flush=True)
    print("=" * 60, flush=True)

    print("\n[1/3] Fetching data from Redshift...", flush=True)
    raw = fetch_all_data()

    print("\n[2/3] Preparing data...")
    data = prepare_raw_data(raw)

    # Log summary
    for key in ['engagement_all', 'engagement_by_type', 'engagement_detail',
                'os_weekly', 'os_by_type', 'os_detail', 'md_weekly',
                'saturation_frequency', 'vertical_overlap', 'value_prop_repetition']:
        count = len(data.get(key, []))
        print(f"  {key}: {count} records")

    print(f"  WTD detected: {data.get('has_wtd', False)}")

    print("\n[3/3] Generating HTML dashboard...")
    html = generate_html(data)

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n  Dashboard saved to: {OUTPUT_PATH}")
    print(f"  Size: {len(html):,} bytes")

    webbrowser.open('file://' + OUTPUT_PATH)
    print("\n  Opened in browser.")
    print("=" * 60)


if __name__ == '__main__':
    main()
