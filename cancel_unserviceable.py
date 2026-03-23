"""
cancel_unserviceable.py — Cancel unserviceable Shopify orders
==============================================================
Fetches all CREATED Shopify shipments, attempts to invoice each one,
and auto-cancels any that fail with NOT_SERVICEABLE.

Usage:
  python3 cancel_unserviceable.py            # Check and cancel all
  python3 cancel_unserviceable.py --dry-run  # Identify only, no cancellations
"""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cancel_unserviceable")


async def main():
    dry_run = "--dry-run" in sys.argv

    log.info("╔══════════════════════════════════════════════╗")
    log.info("║   Cancel Unserviceable Shopify Orders        ║")
    if dry_run:
        log.info("║   DRY RUN — no cancellations will be made   ║")
    log.info("╚══════════════════════════════════════════════╝")

    from unicommerce_api import UnicommerceClient
    from shopify_api import cancel_order

    async with UnicommerceClient(
        os.environ["UNICOMMERCE_USERNAME"],
        os.environ["UNICOMMERCE_PASSWORD"],
        os.environ["UNICOMMERCE_FACILITY"],
    ) as client:

        # ── Fetch all CREATED Shopify shipments ────────────────────────────────
        log.info("Fetching CREATED Shopify shipments…")
        codes, details_map = await client.get_all_codes_for_channel("CREATED", channel="Shopify")
        log.info(f"  Found {len(codes)} Shopify shipments")

        if not codes:
            log.info("  Nothing to process.")
            return

        # ── Attempt invoice to detect unserviceable orders ─────────────────────
        log.info("Checking serviceability…")
        unserviceable = []

        for code in codes:
            try:
                await client.create_invoice_and_label(code)
                log.info(f"  ✓ {code} — serviceable")
            except Exception as e:
                if "NOT_SERVICEABLE" in str(e):
                    detail      = details_map.get(code, {})
                    display_oid = detail.get("order_id", code)
                    shopify_oid = detail.get("shopify_order_id", "")
                    log.warning(f"  ✗ {code} (Order #{display_oid}) — UNSERVICEABLE")
                    unserviceable.append({
                        "shipment_code": code,
                        "order_id":      display_oid,
                        "shopify_oid":   shopify_oid,
                    })
                else:
                    log.error(f"  ✗ {code} — other error: {e}")

        # ── Summary ────────────────────────────────────────────────────────────
        log.info("")
        log.info(f"Found {len(unserviceable)} unserviceable order(s):")
        for o in unserviceable:
            log.info(f"  • Order #{o['order_id']} ({o['shipment_code']})")

        if not unserviceable:
            log.info("  Nothing to cancel.")
            return

        if dry_run:
            log.info("")
            log.info("[DRY RUN] No cancellations made. Remove --dry-run to execute.")
            return

        # ── Cancel unserviceable orders ────────────────────────────────────────
        log.info("")
        log.info("Cancelling unserviceable orders…")
        cancelled = []

        for o in unserviceable:
            if not o["shopify_oid"]:
                log.error(f"  ✗ Order #{o['order_id']} — no Shopify ID found, cancel manually")
                continue

            success = await cancel_order(o["shopify_oid"], o["order_id"])
            if success:
                log.info(f"  ✓ Order #{o['order_id']} cancelled — Shopify will auto-refund the customer")
                cancelled.append(o)
            else:
                log.error(f"  ✗ Order #{o['order_id']} — cancellation failed, check logs")

        log.info("")
        log.info(f"Done. {len(cancelled)}/{len(unserviceable)} order(s) cancelled.")


if __name__ == "__main__":
    asyncio.run(main())
