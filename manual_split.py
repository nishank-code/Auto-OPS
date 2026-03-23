"""
Gimi Michi — Manual Split Tool
================================
Runs the split algorithm on any shipment codes you provide, regardless of
their current status in Unicommerce.

Usage:
  python3 manual_split.py GIMI2121094 GIMI2121730 GIMI2121759
  python3 manual_split.py --dry-run GIMI2121094 GIMI2121730
  python3 manual_split.py --file codes.txt

Options:
  --dry-run       Show what would be split without making any API calls
  --file PATH     Read shipment codes from a text file (one per line)

The script will:
  1. Fetch full details for each code
  2. Run the split algorithm
  3. Call the Unicommerce split API for any that need splitting
  4. Print a summary of what was split and into which new codes
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("manual_split")


def parse_args():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]

    codes = []

    if "--file" in args:
        idx = args.index("--file")
        try:
            file_path = args[idx + 1]
        except IndexError:
            print("ERROR: --file requires a path argument")
            sys.exit(1)
        path = Path(file_path)
        if not path.exists():
            print(f"ERROR: File not found: {file_path}")
            sys.exit(1)
        codes = [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
        args = [a for a in args if a not in ("--file", file_path)]
    
    # Remaining args are shipment codes
    codes += [a for a in args if not a.startswith("--")]

    if not codes:
        print(__doc__)
        sys.exit(1)

    return codes, dry_run


async def main():
    codes, dry_run = parse_args()

    username      = os.environ.get("UNICOMMERCE_USERNAME")
    password      = os.environ.get("UNICOMMERCE_PASSWORD")
    facility_code = os.environ.get("UNICOMMERCE_FACILITY")

    if not all([username, password, facility_code]):
        log.error("Missing credentials — check your .env file")
        sys.exit(1)

    log.info(f"Manual Split Tool {'[DRY RUN] ' if dry_run else ''}— {len(codes)} code(s)")
    log.info(f"Codes: {codes}")

    from unicommerce_api import UnicommerceClient
    from split_shipments_api import orders_needing_split

    async with UnicommerceClient(username, password, facility_code) as client:

        # ── Step 1: Fetch details for all provided codes ───────────────────────
        log.info("\nFetching shipment details…")
        details = []
        for code in codes:
            try:
                detail = await client.get_shipment_details(code)
                details.append(detail)
                log.info(f"  ✓ {code} | channel={detail['channel']} | SKUs={detail['qty_map']}")
            except Exception as e:
                log.error(f"  ✗ {code}: {e}")

        if not details:
            log.error("No valid shipments found. Exiting.")
            sys.exit(1)

        # ── Step 2: Run split algorithm ────────────────────────────────────────
        orders = {
            d["shipment_code"]: {
                "shipment_code": d["shipment_code"],
                "order_id":      d["order_id"],
                "qty_map":       d["qty_map"],
            }
            for d in details
        }

        to_split = orders_needing_split(orders)
        no_split = [code for code in orders if code not in {o["shipment_code"] for o in to_split}]

        log.info(f"\nSplit analysis:")
        log.info(f"  {len(no_split)} shipment(s) don't need splitting: {no_split}")
        log.info(f"  {len(to_split)} shipment(s) need splitting:")
        for o in to_split:
            log.info(f"    {o['shipment_code']} → {len(o['shipments'])} packets:")
            for i, packet in enumerate(o["shipments"], 1):
                log.info(f"      Packet {i}: {packet}")

        if not to_split:
            log.info("\nNothing to split. All done.")
            return

        if dry_run:
            log.info("\n[DRY RUN] No API calls made. Remove --dry-run to execute.")
            return

        # ── Step 3: Execute splits ─────────────────────────────────────────────
        log.info(f"\nExecuting {len(to_split)} split(s)…")
        success, failed = [], []

        for order in to_split:
            code = order["shipment_code"]
            try:
                new_codes = await client.split_shipment(code, order["shipments"])
                if new_codes:
                    success.append(code)
                    log.info(f"  ✓ {code} → {new_codes}")
                else:
                    failed.append(code)
                    log.error(f"  ✗ {code}: API returned no new codes")
            except Exception as e:
                failed.append(code)
                log.error(f"  ✗ {code}: {e}")

        # ── Summary ────────────────────────────────────────────────────────────
        log.info(f"\n{'='*50}")
        log.info(f"Done. {len(success)}/{len(to_split)} split(s) successful.")
        if failed:
            log.warning(f"Failed (split manually in Unicommerce): {failed}")


if __name__ == "__main__":
    asyncio.run(main())
