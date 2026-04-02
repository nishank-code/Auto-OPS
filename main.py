"""
Gimi Michi — Unicommerce Order Automation (API-only)
======================================================
Full CRED + Shopify + Flipkart daily order processing via Unicommerce REST API.
No browser. No UI clicks.

Usage:
  python3 main.py                              # Full run: all channels
  python3 main.py --cred-only                  # CRED only
  python3 main.py --shopify-only               # Shopify only
  python3 main.py --flipkart-only              # Flipkart only
  python3 main.py --skip-cred                  # Shopify + Flipkart
  python3 main.py --skip-shopify               # CRED + Flipkart
  python3 main.py --skip-flipkart              # CRED + Shopify
  python3 main.py --dry-run                    # Count shipments, no changes

Limiters:
  --limit=N            Global: cap all channels to N orders each
  --cred-limit=N       Cap CRED only to N orders
  --shopify-limit=N    Cap Shopify only to N orders
  --flipkart-limit=N   Cap Flipkart only to N orders

  Channel-specific limits take priority over --limit when both are set.
  Examples:
    python3 main.py --limit=5                  # 5 orders per channel
    python3 main.py --shopify-only --limit=10  # 10 Shopify orders only
    python3 main.py --cred-limit=3 --shopify-limit=10 --flipkart-limit=5

Flipkart TAT (dispatch deadline) filter:
  By default Flipkart processes only orders due tomorrow (fulfillmentTat = tomorrow).
  If run between 12:00 AM–1:59 AM, "tomorrow" is treated as today (late-night run).
  Override with explicit date range:
  --flipkart-start-date=DATE   Start of fulfillmentTat range (inclusive)
  --flipkart-end-date=DATE     End of fulfillmentTat range (inclusive); defaults to start date

  DATE accepts: '3 April', '2026-04-03', '3rd April 2026', etc.
  Examples:
    python3 main.py --flipkart-only                                        # tomorrow's TAT (default)
    python3 main.py --flipkart-only --flipkart-start-date="3 April"        # single day
    python3 main.py --flipkart-only --flipkart-start-date="1 April" --flipkart-end-date="5 April"
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
LOG_DIR    = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

DATE_STR   = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = SCRIPT_DIR / "output" / DATE_STR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / f"{DATE_STR}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("main")

# ── Required env vars ──────────────────────────────────────────────────────────
REQUIRED = [
    "UNICOMMERCE_USERNAME",
    "UNICOMMERCE_PASSWORD",
    "UNICOMMERCE_FACILITY",
    "GDRIVE_ROOT_FOLDER_ID",
]

def check_env():
    missing = [v for v in REQUIRED if not os.environ.get(v)]
    if missing:
        log.error(f"Missing .env variables: {', '.join(missing)}")
        sys.exit(1)

# ── Shopify label groups ───────────────────────────────────────────────────────
# Experience boxes are split into three separate files by box type.
# Stickers (GMS20002) and totes (GMS20001) are not used for classification —
# they accompany an experience box and go into that box's file automatically.
# A shipment is assigned to the FIRST group whose primary SKU appears in it.
LABEL_GROUPS = [
    ("OGExpBox",            ["GMK06105", "Experience_Box_Normal"]),
    ("CurryChickenExpBox",  ["GMK04306", "Experience_Box_Curry_Chicken_Shopify"]),
    ("VeggieExpBox",        ["GMK05106"]),
    ("CheesyExpBox",        ["Experience_Box_Cheesy_Shopify"]),
    ("4Packs", [
        # Shopify GMK 4-pack and 8-pack codes
        "GMK00104", "GMK00204", "GMK00304", "GMK00404", "GMK00504",
        "GMK01104", "GMK01204", "GMK00108", "GMK00208", "GMS00301",
        # Shopify legacy pack SKUs
        "Hot_Kimchi-4", "Korean_Spicy-4", "Hot_Kimchi-2_and_Korean_Spicy-2",
        "Hot_Chicken-2_and_Curry_Chicken-2", "Hot_Chicken-4",
        "Crazy_Cheesy_4", "Curry_Chicken_4",
        "Hot_Kimchi-8", "Korean_Spicy-8",
    ]),
    ("6Packs", [
        "GMK02106",
        "GMK06205",    # 5 Flavour Pack
        "GMK01306",    # Hot Chicken 3 + Curry Chicken 3
        "Korean_Kimchi-2_Korean_Spicy-2_Crazy_Cheesy-2",
        # Flipkart 6-pack and 12-pack SKUs
        "Hot_Kimchi-6", "Hot_Kimchi-6_and_Korean_Spicy-6",
    ]),
]

def classify_shipment(sku_set: set) -> str:
    for group_name, keywords in LABEL_GROUPS:
        if any(kw in sku_set for kw in keywords):
            return group_name
    return "OGExpBox"  # fallback for unknown experience box types

# Shopify PDF filenames use the original group names (pre-Flipkart rename)
SHOPIFY_GROUP_DISPLAY = {
    "OGExpBox":           "5FlavourExpBox",
    "CurryChickenExpBox": "3+3ChickenExpBox",
    "VeggieExpBox":       "VeggieExpBox",
    "CheesyExpBox":       "CheeseExpBox",
    "4Packs":             "4_Pack",
    "6Packs":             "6_Pack",
}


def filter_by_date(codes: list, details_map: dict, filter_date) -> list:
    """Filter shipment codes to only those whose order was placed on filter_date."""
    if not filter_date:
        return codes
    from dateutil import parser as dateparser
    matched = []
    for code in codes:
        raw = details_map.get(code, {}).get("order_date", "")
        if not raw:
            continue
        try:
            order_date = dateparser.parse(raw).date()
            if order_date == filter_date:
                matched.append(code)
        except Exception:
            pass
    log.info(f"  Date filter ({filter_date.strftime('%d %b %Y')}): {len(matched)}/{len(codes)} orders match")
    return matched


def get_flipkart_default_tat_date() -> date:
    """
    Returns the default dispatch-deadline date to target for Flipkart.
    Normally this is tomorrow, but if the script is running between
    12:00 AM and 1:59 AM we treat today as the target (late-night run
    that belongs to the previous working session).
    """
    now = datetime.now()
    if now.hour < 2:
        return now.date()
    return now.date() + timedelta(days=1)


def filter_by_fulfillment_tat(
    codes: list,
    details_map: dict,
    start_date: date,
    end_date: date,
) -> list:
    """Filter Flipkart shipment codes to those whose fulfillmentTat falls within [start_date, end_date]."""
    matched = []
    for code in codes:
        tat_ms = details_map.get(code, {}).get("fulfillment_tat")
        if not tat_ms:
            continue
        tat_date = datetime.fromtimestamp(tat_ms / 1000).date()
        if start_date <= tat_date <= end_date:
            matched.append(code)
    label = start_date.strftime("%d %b %Y") if start_date == end_date else f"{start_date.strftime('%d %b %Y')} – {end_date.strftime('%d %b %Y')}"
    log.info(f"  TAT filter ({label}): {len(matched)}/{len(codes)} Flipkart orders match")
    return matched


# ═══════════════════════════════════════════════════════════════════════════════
#  CRED FLOW
#  - No splitting
#  - Single manifest for all CRED shipments
#  - CRED_Invoices.pdf — all invoices merged
#  - CRED_{Courier}_Labels.pdf — one file per courier assigned by CRED
# ═══════════════════════════════════════════════════════════════════════════════
async def run_cred_flow(client, dry_run: bool, limit: int = None, filter_date=None, all_details: dict = None) -> list[str]:
    import base64
    from pdf_utils import merge_pdfs, save_pdf
    errors = []

    log.info("Fetching CRED CREATED shipments…")
    codes, details_map = await client.get_all_codes_for_channel("CREATED", channel="CRED", all_details=all_details)
    log.info(f"  Found {len(codes)} CRED shipments")

    if filter_date:
        codes = filter_by_date(codes, details_map, filter_date)

    if not codes:
        log.info("  No CRED orders matched — skipping")
        return errors

    if limit:
        codes = codes[:limit]
        log.info(f"  [LIMIT] Capped to first {limit}: {codes}")

    if dry_run:
        log.info(f"  [DRY RUN] Would process: {codes}")
        return errors

    # ── Process all codes concurrently ────────────────────────────────────────
    CONCURRENCY = 15
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _process_one_cred(code):
        async with sem:
            result    = await client.create_invoice_and_label(code)
            label_url = result.get("shippingLabelLink")
            label_b64 = result.get("label", "")

            async def _get_label():
                if label_b64:
                    return base64.b64decode(label_b64), "response"
                if label_url:
                    return await client.get_label_pdf(label_url), "url"
                return None, None

            invoice_pdf, (label_pdf, label_src) = await asyncio.gather(
                client.get_invoice_pdf(code),
                _get_label(),
            )

            prov_code = result.get("shippingProviderCode", "")
            detail    = details_map.get(code, {})
            if not prov_code:
                prov_code = detail.get("shipping_provider", "")
            method       = detail.get("shipping_method", "STD") or "STD"
            courier_name = _normalise_courier(prov_code) if prov_code else "Unknown"

            return {
                "code":         code,
                "invoice_num":  result.get("invoiceDisplayCode"),
                "invoice_pdf":  invoice_pdf,
                "label_pdf":    label_pdf,
                "label_src":    label_src,
                "courier_name": courier_name,
                "prov_code":    prov_code,
                "method":       method,
            }

    log.info(f"  Creating invoices + labels for {len(codes)} CRED orders…")
    raw_results = await asyncio.gather(
        *[_process_one_cred(c) for c in codes],
        return_exceptions=True,
    )

    invoice_pdfs                    = []
    courier_labels: dict[str, list] = {}
    manifest_codes                  = []
    manifest_provider_code          = ""
    manifest_method_code            = "STD"

    for code, r in zip(codes, raw_results):
        if isinstance(r, Exception):
            log.error(f"  ✗ {code}: {type(r).__name__}: {r}")
            errors.append(f"CRED invoice+label {code}: {type(r).__name__}: {r}")
        else:
            log.info(f"  ✓ Invoice: {r['code']} → {r['invoice_num']}")
            if r["label_pdf"]:
                log.info(f"  ✓ Label:   {r['code']} (from {r['label_src']})")
            else:
                log.warning(f"  ⚠ No label for {r['code']}")
            if r["invoice_pdf"]:
                invoice_pdfs.append(r["invoice_pdf"])
            if r["label_pdf"]:
                courier_labels.setdefault(r["courier_name"], []).append(r["label_pdf"])
            manifest_codes.append(r["code"])
            if not manifest_provider_code and r["prov_code"]:
                manifest_provider_code = r["prov_code"]
                manifest_method_code   = r["method"]

    # ── Save PDFs ──────────────────────────────────────────────────────────────
    if invoice_pdfs:
        save_pdf(merge_pdfs(invoice_pdfs), OUTPUT_DIR / "CRED_Invoices.pdf")

    for courier_name, pdfs in courier_labels.items():
        save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / f"CRED_{courier_name}_Labels_{len(pdfs)}.pdf")
        log.info(f"  Saved: CRED_{courier_name}_Labels_{len(pdfs)}.pdf ({len(pdfs)} labels)")

    # ── Single manifest for all CRED shipments ─────────────────────────────────
    if manifest_codes:
        log.info(f"Creating CRED manifest for {len(manifest_codes)} shipments…")
        try:
            _, _ = await client.create_and_complete_manifest(
                channel="CRED",
                shipping_provider_code=manifest_provider_code or "UNKNOWN",
                shipping_provider_name=manifest_provider_code or "UNKNOWN",
                shipping_method_code=manifest_method_code,
                shipment_codes=manifest_codes,
                third_party_shipping=True,
                is_aggregator=False,
            )
        except Exception as e:
            log.error(f"  CRED manifest failed: {e}")
            errors.append(f"CRED manifest: {e}")

    return errors


def _normalise_courier(raw: str) -> str:
    mapping = {
        "bluedart": "Bluedart", "xpressbees": "XpressBees",
        "delhivery": "Delhivery", "ekart": "Ekart",
        "dtdc": "DTDC", "shadowfax": "Shadowfax",
        "ecom": "Ecom", "proship": "Proship",
    }
    return mapping.get(raw.lower().replace(" ", ""), raw.replace(" ", "_"))


# ═══════════════════════════════════════════════════════════════════════════════
#  FLIPKART FLOW
#  - No splitting (Flipkart sends single-item orders)
#  - Courier assigned by Flipkart (like CRED)
#  - Single manifest for all Flipkart shipments
#  - Flipkart_Invoices.pdf — all invoices merged
#  - Flipkart_Labels.pdf — all labels in one file
# ═══════════════════════════════════════════════════════════════════════════════
async def run_flipkart_flow(
    client,
    dry_run: bool,
    limit: int = None,
    filter_date=None,
    all_details: dict = None,
    tat_start: date = None,
    tat_end: date = None,
) -> list[str]:
    import base64
    from pdf_utils import merge_pdfs, save_pdf
    errors = []

    log.info("Fetching Flipkart CREATED shipments…")
    codes, details_map = await client.get_all_codes_for_channel("CREATED", channel="FLIPKART", all_details=all_details)
    log.info(f"  Found {len(codes)} Flipkart shipments")

    if filter_date:
        codes = filter_by_date(codes, details_map, filter_date)

    # TAT (fulfillment deadline) filter — always applied for Flipkart;
    # tat_start/tat_end are pre-computed by main() with the right defaults.
    codes = filter_by_fulfillment_tat(codes, details_map, tat_start, tat_end)

    if not codes:
        log.info("  No Flipkart orders matched — skipping")
        return errors

    if limit:
        codes = codes[:limit]
        log.info(f"  [LIMIT] Capped to first {limit}: {codes}")

    if dry_run:
        log.info(f"  [DRY RUN] Would process: {codes}")
        return errors

    # ── Process all codes concurrently ────────────────────────────────────────
    CONCURRENCY = 15
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _process_one_flipkart(code):
        async with sem:
            result    = await client.create_invoice_and_label(code)
            label_url = result.get("shippingLabelLink")
            label_b64 = result.get("label", "")

            async def _get_label():
                if label_b64:
                    return base64.b64decode(label_b64), "response"
                if label_url:
                    return await client.get_label_pdf(label_url), "url"
                return None, None

            invoice_pdf, (label_pdf, label_src) = await asyncio.gather(
                client.get_invoice_pdf(code),
                _get_label(),
            )

            prov_code = result.get("shippingProviderCode", "")
            detail    = details_map.get(code, {})
            if not prov_code:
                prov_code = detail.get("shipping_provider", "")
            method  = detail.get("shipping_method", "STD") or "STD"
            qty_map = detail.get("qty_map", {})
            group   = classify_shipment(set(qty_map.keys())) if label_pdf else None

            return {
                "code":        code,
                "invoice_num": result.get("invoiceDisplayCode"),
                "invoice_pdf": invoice_pdf,
                "label_pdf":   label_pdf,
                "label_src":   label_src,
                "prov_code":   prov_code,
                "method":      method,
                "group":       group,
            }

    log.info(f"  Creating invoices + labels for {len(codes)} Flipkart orders…")
    raw_results = await asyncio.gather(
        *[_process_one_flipkart(c) for c in codes],
        return_exceptions=True,
    )

    invoice_pdfs           = []
    group_pdfs             = {g: [] for g, _ in LABEL_GROUPS}
    manifest_codes         = []
    manifest_provider_code = ""
    manifest_method_code   = "STD"

    for code, r in zip(codes, raw_results):
        if isinstance(r, Exception):
            log.error(f"  ✗ {code}: {type(r).__name__}: {r}")
            errors.append(f"Flipkart invoice+label {code}: {type(r).__name__}: {r}")
        else:
            log.info(f"  ✓ Invoice: {r['code']} → {r['invoice_num']}")
            if r["label_pdf"]:
                log.info(f"  ✓ Label+Group: {r['code']} → '{r['group']}' (from {r['label_src']})")
            else:
                log.warning(f"  ⚠ No label for {r['code']}")
            if r["invoice_pdf"]:
                invoice_pdfs.append(r["invoice_pdf"])
            if r["label_pdf"] and r["group"]:
                group_pdfs.setdefault(r["group"], []).append(r["label_pdf"])
            manifest_codes.append(r["code"])
            if not manifest_provider_code and r["prov_code"]:
                manifest_provider_code = r["prov_code"]
                manifest_method_code   = r["method"]

    # ── Save PDFs ──────────────────────────────────────────────────────────────
    if invoice_pdfs:
        save_pdf(merge_pdfs(invoice_pdfs), OUTPUT_DIR / "Flipkart_Invoices.pdf")

    for group_name, pdfs in group_pdfs.items():
        if pdfs:
            save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / f"Flipkart_Labels_{group_name}_{len(pdfs)}.pdf")
            log.info(f"  Saved: Flipkart_Labels_{group_name}_{len(pdfs)}.pdf ({len(pdfs)} labels)")

    # ── Single manifest for all Flipkart shipments ─────────────────────────────
    if manifest_codes:
        log.info(f"Creating Flipkart manifest for {len(manifest_codes)} shipments…")
        try:
            _, _ = await client.create_and_complete_manifest(
                channel="FLIPKART",
                shipping_provider_code=manifest_provider_code or "UNKNOWN",
                shipping_provider_name=manifest_provider_code or "UNKNOWN",
                shipping_method_code="",
                shipment_codes=manifest_codes,
                third_party_shipping=True,
                is_aggregator=False,
            )
        except Exception as e:
            log.error(f"  Flipkart manifest failed: {e}")
            errors.append(f"Flipkart manifest: {e}")

    return errors


# ═══════════════════════════════════════════════════════════════════════════════
#  SHOPIFY FLOW
# ═══════════════════════════════════════════════════════════════════════════════
async def run_shopify_flow(client, dry_run: bool, limit: int = None, filter_date=None, all_details: dict = None) -> list[str]:
    from pdf_utils import merge_pdfs, save_pdf
    errors = []

    log.info("Fetching Shopify CREATED shipments…")
    codes, initial_sku_map = await client.get_all_codes_for_channel("CREATED", channel="Shopify", all_details=all_details)
    log.info(f"  Found {len(codes)} Shopify shipments")

    if filter_date:
        codes = filter_by_date(codes, initial_sku_map, filter_date)

    if not codes:
        log.info("  No Shopify orders matched — skipping")
        return errors

    # Apply limit BEFORE split so we only split the limited set
    if limit:
        codes = codes[:limit]
        log.info(f"  [LIMIT] Capped to first {limit}: {codes}")

    if dry_run:
        log.info(f"  [DRY RUN] Would process: {codes}")
        return errors

    # ── Step 2: Run split algorithm ────────────────────────────────────────────
    log.info("Running split algorithm…")
    details = [
        {
            "shipment_code": code,
            "order_id":      initial_sku_map.get(code, {}).get("order_id", code),
            "qty_map":       initial_sku_map.get(code, {}).get("qty_map", {}),
        }
        for code in codes
    ]
    try:
        sku_map, child_to_parent = await run_split(client, details)
    except Exception as e:
        log.error(f"Split failed: {e}")
        errors.append(f"Shopify split: {e}")
        return errors

    # Save SKU map for debug reference
    (OUTPUT_DIR / "shipment_sku_map.json").write_text(
        json.dumps(sku_map, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # ── Step 3: Post-split codes (no re-fetch needed) ─────────────────────────
    # sku_map already has the exact final codes: originals that didn't split +
    # new child codes returned by the split API. Use it directly.
    codes = list(sku_map.keys())
    log.info(f"  {len(codes)} shipments to process (from split result)")

    # ── Step 4: Invoice + label — parallel with concurrency cap ───────────────
    log.info("Creating invoices + labels…")
    import base64
    import time

    INVOICE_CONCURRENCY = 3
    sem = asyncio.Semaphore(INVOICE_CONCURRENCY)
    order_times: list[float] = []   # per-order elapsed seconds (for sequential estimate)

    async def _process_one(code):
        async with sem:
            _t0 = time.monotonic()
            result    = await client.create_invoice_and_label(code)
            label_url = result.get("shippingLabelLink")
            label_b64 = result.get("label", "")

            async def _get_label():
                if label_url:
                    return await client.get_label_pdf(label_url)
                if label_b64:
                    return base64.b64decode(label_b64)
                return None

            # Fetch invoice PDF and download label concurrently
            invoice_pdf, label_pdf = await asyncio.gather(
                client.get_invoice_pdf(code),
                _get_label(),
            )

            order_times.append(time.monotonic() - _t0)

            prov_code = result.get("shippingProviderCode", "")
            if not prov_code:
                parent    = child_to_parent.get(code, code)
                prov_code = initial_sku_map.get(parent, {}).get("shipping_provider", "") or "PROSHIP"

            group = None
            if label_pdf:
                entry   = sku_map.get(code, {})
                qty_map = entry.get("qty_map", entry) if isinstance(entry, dict) else {}
                group   = classify_shipment(set(qty_map.keys()))

            return {
                "code":        code,
                "invoice_num": result.get("invoiceDisplayCode"),
                "invoice_pdf": invoice_pdf,
                "label_pdf":   label_pdf,
                "prov_code":   prov_code.upper(),
                "group":       group,
            }

    _parallel_t0 = time.monotonic()
    raw_results = await asyncio.gather(
        *[_process_one(c) for c in codes],
        return_exceptions=True,
    )
    _parallel_elapsed = time.monotonic() - _parallel_t0
    _sequential_estimate = sum(order_times)
    _speedup = _sequential_estimate / _parallel_elapsed if _parallel_elapsed > 0 else 0
    log.info(
        f"  ⏱  Parallel wall-clock: {_parallel_elapsed:.1f}s | "
        f"Sequential estimate: {_sequential_estimate:.1f}s | "
        f"Speedup: {_speedup:.1f}×  (concurrency={INVOICE_CONCURRENCY})"
    )

    invoice_pdfs = []
    group_pdfs   = {g: [] for g, _ in LABEL_GROUPS}
    dispatching: dict[str, list[str]] = {}
    auto_cancelled = []

    for code, r in zip(codes, raw_results):
        if isinstance(r, Exception):
            err_str = str(r)
            if "NOT_SERVICEABLE" in err_str:
                parent_code = child_to_parent.get(code, code)
                detail      = initial_sku_map.get(parent_code, {})
                shopify_oid = detail.get("shopify_order_id", "")
                display_oid = detail.get("order_id", code)
                log.warning(f"  ⚠ {code} (Order #{display_oid}): pincode unserviceable — auto-cancelling…")
                if shopify_oid:
                    from shopify_api import cancel_order
                    success = await cancel_order(shopify_oid, display_oid)
                    if success:
                        log.info(f"  ✓ Order #{display_oid} cancelled on Shopify — customer will be auto-refunded")
                        auto_cancelled.append({"order_id": display_oid, "shipment_code": code})
                    else:
                        errors.append(f"Shopify cancel/refund failed for order #{display_oid} ({code})")
                else:
                    log.error(f"  ✗ No Shopify order ID found for {code} — manual cancellation required")
                    errors.append(f"Shopify NOT_SERVICEABLE (no order ID) {code}")
            else:
                log.error(f"  ✗ {code}: {type(r).__name__}: {r}")
                errors.append(f"Shopify invoice+label {code}: {type(r).__name__}: {r}")
        else:
            log.info(f"  ✓ Invoice: {r['code']} → {r['invoice_num']}")
            if r["label_pdf"]:
                log.info(f"  ✓ Label+Group: {r['code']} → '{r['group']}'")
            else:
                log.warning(f"  ⚠ No label for {r['code']}")
            if r["invoice_pdf"]:
                invoice_pdfs.append(r["invoice_pdf"])
            if r["label_pdf"] and r["group"]:
                group_pdfs.setdefault(r["group"], []).append(r["label_pdf"])
            dispatching.setdefault(r["prov_code"], []).append(r["code"])

    # ── Send WhatsApp notification for all cancellations ──────────────────────
    if auto_cancelled:
        log.info(f"  {len(auto_cancelled)} order(s) auto-cancelled due to unserviceable pincode.")

    # ── Step 5: Save PDFs ──────────────────────────────────────────────────────
    if invoice_pdfs:
        save_pdf(merge_pdfs(invoice_pdfs), OUTPUT_DIR / "Shopify_Invoices.pdf")

    for group_name, pdfs in group_pdfs.items():
        if pdfs:
            display = SHOPIFY_GROUP_DISPLAY.get(group_name, group_name)
            save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / f"Shopify_Labels_{display}_{len(pdfs)}.pdf")
            log.info(f"  Saved: Shopify_Labels_{display}_{len(pdfs)}.pdf ({len(pdfs)} labels)")

    # ── Step 6: One manifest per provider group, retry excluded codes ────────────
    total_dispatching = sum(len(v) for v in dispatching.values())
    if dispatching:
        log.info(f"Creating Shopify manifest(s) for {total_dispatching} shipments across {len(dispatching)} provider(s)…")
        all_excluded = []

        for prov_code, prov_codes in dispatching.items():
            try:
                _, failed_codes = await client.create_and_complete_manifest(
                    channel="SHOPIFY",
                    shipping_provider_code=prov_code,
                    shipping_provider_name=prov_code,
                    shipping_method_code="",
                    shipment_codes=prov_codes,
                    third_party_shipping=False,
                    is_aggregator=True,
                    shipping_courier=prov_code,
                )
                log.info(f"  ✓ Manifest created for '{prov_code}' ({len(prov_codes) - len(failed_codes)}/{len(prov_codes)} shipments)")
                if failed_codes:
                    all_excluded.extend(failed_codes)
            except Exception as e:
                log.error(f"  Shopify manifest failed for '{prov_code}': {e}")
                errors.append(f"Shopify manifest {prov_code}: {e}")

        # ── Retry excluded codes by fetching their actual assigned carrier ────
        if all_excluded:
            log.info(f"  Retrying {len(all_excluded)} excluded code(s) with their actual carrier…")
            retry_by_provider: dict[str, list[str]] = {}
            for code in all_excluded:
                try:
                    detail = await client.get_shipment_details(code)
                    actual_prov = (detail.get("shipping_provider") or "PROSHIP").upper()
                    retry_by_provider.setdefault(actual_prov, []).append(code)
                    log.info(f"    {code} → actual carrier: {actual_prov}")
                except Exception as e:
                    log.error(f"    Could not fetch carrier for {code}: {e}")

            for prov_code, retry_codes in retry_by_provider.items():
                try:
                    _, still_failed = await client.create_and_complete_manifest(
                        channel="SHOPIFY",
                        shipping_provider_code=prov_code,
                        shipping_provider_name=prov_code,
                        shipping_method_code="",
                        shipment_codes=retry_codes,
                        third_party_shipping=False,
                        is_aggregator=True,
                        shipping_courier=prov_code,
                    )
                    log.info(f"  ✓ Retry manifest for '{prov_code}' ({len(retry_codes) - len(still_failed)}/{len(retry_codes)} added)")
                    if still_failed:
                        for c in still_failed:
                            log.error(f"  ✗ {c} still excluded — manual dispatch required")
                            errors.append(f"Shopify dispatch {c}: excluded from manifest after retry")
                except Exception as e:
                    log.error(f"  Retry manifest failed for '{prov_code}': {e}")
                    errors.append(f"Shopify retry manifest {prov_code}: {e}")

    return errors


# ═══════════════════════════════════════════════════════════════════════════════
#  SPLIT HELPER
# ═══════════════════════════════════════════════════════════════════════════════
async def run_split(client, details: list[dict]) -> tuple[dict, dict]:
    """
    Runs the split algorithm on in-memory shipment details.
    Calls the Unicommerce split API for orders that need splitting.
    Returns:
      sku_map        — {shipment_code: {sku: qty}} for ALL resulting shipments
      child_to_parent— {child_code: parent_code} for all split children
    """
    from split_shipments_api import orders_needing_split

    orders = {
        d["shipment_code"]: {
            "shipment_code": d["shipment_code"],
            "order_id":      d["order_id"],
            "qty_map":       d["qty_map"],
        }
        for d in details
    }

    to_split    = orders_needing_split(orders)
    split_codes = {o["shipment_code"] for o in to_split}
    log.info(f"  {len(orders)} orders, {len(to_split)} need splitting")

    # Start with orders that don't need splitting
    all_shipments   = {
        code: order["qty_map"]
        for code, order in orders.items()
        if code not in split_codes
    }
    child_to_parent: dict[str, str] = {}

    for order in to_split:
        code = order["shipment_code"]
        log.info(f"  Splitting {code}…")
        try:
            new_codes = await client.split_shipment(code, order["shipments"])
            if new_codes:
                for packet_str, new_code in new_codes.items():
                    idx = int(packet_str) - 1
                    all_shipments[new_code] = (
                        order["shipments"][idx]
                        if idx < len(order["shipments"])
                        else {}
                    )
                    child_to_parent[new_code] = code  # track parentage
            else:
                all_shipments[code] = order["qty_map"]
        except Exception as e:
            log.error(f"  Split failed for {code}: {e}")
            all_shipments[code] = order["qty_map"]

    return all_shipments, child_to_parent


# ═══════════════════════════════════════════════════════════════════════════════
#  GOOGLE DRIVE UPLOAD
# ═══════════════════════════════════════════════════════════════════════════════
def upload_to_drive():
    from gdrive import GDriveUploader

    uploader  = GDriveUploader(
        root_folder_id=os.environ["GDRIVE_ROOT_FOLDER_ID"],
        token_file=os.environ.get("GDRIVE_TOKEN_FILE", str(SCRIPT_DIR / "token.json")),
    )
    folder_id = uploader.get_or_create_date_folder(DATE_STR)
    pdfs      = sorted(OUTPUT_DIR.glob("*.pdf"))

    if not pdfs:
        log.info("  No PDFs to upload today")
        return

    for pdf in pdfs:
        uploader.upload_file(pdf, folder_id)
        log.info(f"  ↑ {pdf.name}")
    log.info(f"  ✅ {len(pdfs)} PDFs uploaded → Drive/{DATE_STR}/")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════════
async def main():
    check_env()

    dry_run       = "--dry-run"       in sys.argv
    cred_only     = "--cred-only"     in sys.argv
    shopify_only  = "--shopify-only"  in sys.argv
    flipkart_only = "--flipkart-only" in sys.argv
    skip_cred     = "--skip-cred"     in sys.argv
    skip_shopify  = "--skip-shopify"  in sys.argv
    skip_flipkart = "--skip-flipkart" in sys.argv
    global_limit        = None
    cred_limit          = None
    shopify_limit       = None
    flipkart_limit      = None
    filter_date         = None   # datetime.date object if --date is set
    flipkart_tat_start  = None   # explicit TAT start date for Flipkart
    flipkart_tat_end    = None   # explicit TAT end date for Flipkart

    for arg in sys.argv:
        if arg.startswith("--limit="):
            try:
                global_limit = int(arg.split("=")[1])
            except ValueError:
                log.error(f"Invalid --limit value: {arg}")
                sys.exit(1)
        elif arg.startswith("--cred-limit="):
            try:
                cred_limit = int(arg.split("=")[1])
            except ValueError:
                log.error(f"Invalid --cred-limit value: {arg}")
                sys.exit(1)
        elif arg.startswith("--shopify-limit="):
            try:
                shopify_limit = int(arg.split("=")[1])
            except ValueError:
                log.error(f"Invalid --shopify-limit value: {arg}")
                sys.exit(1)
        elif arg.startswith("--flipkart-limit="):
            try:
                flipkart_limit = int(arg.split("=")[1])
            except ValueError:
                log.error(f"Invalid --flipkart-limit value: {arg}")
                sys.exit(1)
        elif arg.startswith("--date="):
            raw_date = arg.split("=", 1)[1]
            try:
                from dateutil import parser as dateparser
                filter_date = dateparser.parse(raw_date, dayfirst=True).date()
                log.info(f"Date filter: {filter_date.strftime('%d %B %Y')}")
            except Exception:
                log.error(f"Could not parse date: '{raw_date}'. Try formats like '22 March', '22nd March 2026', '2026-03-22'")
                sys.exit(1)
        elif arg.startswith("--flipkart-start-date="):
            raw_date = arg.split("=", 1)[1]
            try:
                from dateutil import parser as dateparser
                flipkart_tat_start = dateparser.parse(raw_date, dayfirst=True).date()
            except Exception:
                log.error(f"Could not parse --flipkart-start-date: '{raw_date}'. Try formats like '3 April', '2026-04-03'")
                sys.exit(1)
        elif arg.startswith("--flipkart-end-date="):
            raw_date = arg.split("=", 1)[1]
            try:
                from dateutil import parser as dateparser
                flipkart_tat_end = dateparser.parse(raw_date, dayfirst=True).date()
            except Exception:
                log.error(f"Could not parse --flipkart-end-date: '{raw_date}'. Try formats like '5 April', '2026-04-05'")
                sys.exit(1)

    # Per-channel limits: channel-specific takes priority, global is fallback
    effective_cred_limit     = cred_limit     or global_limit
    effective_shopify_limit  = shopify_limit  or global_limit
    effective_flipkart_limit = flipkart_limit or global_limit

    # Flipkart TAT date range — default to tomorrow (or today if running 12–2 AM)
    if flipkart_tat_start is None:
        flipkart_tat_start = get_flipkart_default_tat_date()
    if flipkart_tat_end is None:
        flipkart_tat_end = flipkart_tat_start

    # --only and --skip flags are mutually exclusive
    exclusives = sum([cred_only, shopify_only, flipkart_only])
    if exclusives > 1:
        log.error("Only one of --cred-only / --shopify-only / --flipkart-only can be used at a time")
        sys.exit(1)
    if exclusives > 0 and any([skip_cred, skip_shopify, skip_flipkart]):
        log.error("--only and --skip flags cannot be used together")
        sys.exit(1)

    # Resolve which channels to run
    run_cred     = (cred_only     or (not shopify_only and not flipkart_only)) and not skip_cred
    run_shopify  = (shopify_only  or (not cred_only    and not flipkart_only)) and not skip_shopify
    run_flipkart = (flipkart_only or (not cred_only    and not shopify_only))  and not skip_flipkart

    active = [c for c, r in [("CRED", run_cred), ("Shopify", run_shopify), ("Flipkart", run_flipkart)] if r]
    mode   = " + ".join(active) if active else "None"
    limit_parts = []
    if effective_cred_limit and run_cred:
        limit_parts.append(f"CRED:{effective_cred_limit}")
    if effective_shopify_limit and run_shopify:
        limit_parts.append(f"Shopify:{effective_shopify_limit}")
    if effective_flipkart_limit and run_flipkart:
        limit_parts.append(f"Flipkart:{effective_flipkart_limit}")
    if limit_parts:
        mode += f" · limit({', '.join(limit_parts)})"
    if filter_date:
        mode += f" · date={filter_date.strftime('%d %b %Y')}"
    if run_flipkart:
        if flipkart_tat_start == flipkart_tat_end:
            mode += f" · fk-tat={flipkart_tat_start.strftime('%d %b %Y')}"
        else:
            mode += f" · fk-tat={flipkart_tat_start.strftime('%d %b %Y')}–{flipkart_tat_end.strftime('%d %b %Y')}"
    if dry_run:
        mode += " · DRY RUN"

    log.info("╔══════════════════════════════════════════════╗")
    log.info("║    Gimi Michi Order Automation  (API-only)   ║")
    log.info(f"║    {DATE_STR}  [{mode}]")
    log.info("╚══════════════════════════════════════════════╝")

    from unicommerce_api import UnicommerceClient

    all_errors = []

    async with UnicommerceClient(
        username=os.environ["UNICOMMERCE_USERNAME"],
        password=os.environ["UNICOMMERCE_PASSWORD"],
        facility_code=os.environ["UNICOMMERCE_FACILITY"],
    ) as client:

        # ── Fetch ALL shipment details once, shared across all channel flows ──
        # This avoids re-fetching 400+ details per channel (would be 3× otherwise).
        log.info("")
        log.info("━━━━━━━━━━━━━━━━━━ FETCHING SHIPMENTS ━━━━━━━━━━━━━━")
        try:
            all_details = await client.get_all_details("CREATED")
        except Exception as e:
            log.exception(f"❌ Failed to fetch shipment details: {e}")
            sys.exit(1)

        # ── CRED ──────────────────────────────────────────────────────────────
        log.info("")
        log.info("━━━━━━━━━━━━━━━━━━━━ CRED FLOW ━━━━━━━━━━━━━━━━━━━━")
        if not run_cred:
            log.info("  Skipped")
        else:
            try:
                errs = await run_cred_flow(client, dry_run, limit=effective_cred_limit, filter_date=filter_date, all_details=all_details)
                all_errors.extend(errs)
                if not errs:
                    log.info("✅ CRED flow complete")
            except Exception as e:
                log.exception(f"❌ CRED flow crashed: {e}")
                all_errors.append(f"CRED: {e}")

        # ── Shopify ───────────────────────────────────────────────────────────
        log.info("")
        log.info("━━━━━━━━━━━━━━━━━━━ SHOPIFY FLOW ━━━━━━━━━━━━━━━━━━")
        if not run_shopify:
            log.info("  Skipped")
        else:
            try:
                errs = await run_shopify_flow(client, dry_run, limit=effective_shopify_limit, filter_date=filter_date, all_details=all_details)
                all_errors.extend(errs)
                if not errs:
                    log.info("✅ Shopify flow complete")
            except Exception as e:
                log.exception(f"❌ Shopify flow crashed: {e}")
                all_errors.append(f"Shopify: {e}")

        # ── Flipkart ──────────────────────────────────────────────────────────
        log.info("")
        log.info("━━━━━━━━━━━━━━━━━━ FLIPKART FLOW ━━━━━━━━━━━━━━━━━━")
        if not run_flipkart:
            log.info("  Skipped")
        else:
            try:
                errs = await run_flipkart_flow(client, dry_run, limit=effective_flipkart_limit, filter_date=filter_date, all_details=all_details, tat_start=flipkart_tat_start, tat_end=flipkart_tat_end)
                all_errors.extend(errs)
                if not errs:
                    log.info("✅ Flipkart flow complete")
            except Exception as e:
                log.exception(f"❌ Flipkart flow crashed: {e}")
                all_errors.append(f"Flipkart: {e}")

    # ── Google Drive ──────────────────────────────────────────────────────────
    log.info("")
    log.info("━━━━━━━━━━━━━━━━━━ GOOGLE DRIVE UPLOAD ━━━━━━━━━━━━━━")
    try:
        upload_to_drive()
    except Exception as e:
        log.exception(f"❌ Drive upload failed: {e}")
        all_errors.append(f"Drive: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━ DONE ━━━━━━━━━━━━━━━━━━━━━━━━")
    if all_errors:
        log.warning(f"Completed with {len(all_errors)} issue(s):")
        for err in all_errors:
            log.warning(f"  • {err}")
        sys.exit(1)
    else:
        log.info("All flows completed successfully ✅")


if __name__ == "__main__":
    asyncio.run(main())
