"""
One-off: download labels for all READY_TO_SHIP shipments for a given channel,
group by SKU type, merge PDFs, and upload to Google Drive.

Usage:
  python3 fetch_rts_labels.py                    # Shopify (default)
  python3 fetch_rts_labels.py --channel=FLIPKART
  python3 fetch_rts_labels.py --channel=Shopify
"""
import asyncio, sys, logging, base64, os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
sys.path.insert(0, str(Path(__file__).parent))

from unicommerce_api import UnicommerceClient
from pdf_utils import merge_pdfs, save_pdf
from gdrive import GDriveUploader

DATE_STR   = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = Path(__file__).parent / "output" / DATE_STR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("rts_labels")

LABEL_GROUPS = [
    ("OGExpBox",           ["GMK06105", "Experience_Box_Normal"]),
    ("CurryChickenExpBox", ["GMK04306", "Experience_Box_Curry_Chicken_Shopify"]),
    ("VeggieExpBox",       ["GMK05106"]),
    ("CheesyExpBox",       ["Experience_Box_Cheesy_Shopify"]),
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

def classify_shipment(sku_set):
    for group_name, keywords in LABEL_GROUPS:
        if any(kw in sku_set for kw in keywords):
            return group_name
    return "OGExpBox"


async def main():
    channel = "Shopify"
    for arg in sys.argv[1:]:
        if arg.startswith("--channel="):
            channel = arg.split("=", 1)[1]

    log.info(f"Channel: {channel}")

    async with UnicommerceClient(
        username=os.environ["UNICOMMERCE_USERNAME"],
        password=os.environ["UNICOMMERCE_PASSWORD"],
        facility_code=os.environ["UNICOMMERCE_FACILITY"],
    ) as client:

        # ── Step 1: All RTS codes for the channel ─────────────────────────────
        log.info(f"Fetching READY_TO_SHIP {channel} shipments…")
        rts_codes, details_map = await client.get_all_codes_for_channel(
            "READY_TO_SHIP", channel=channel
        )
        log.info(f"  {len(rts_codes)} shipments")

        if not rts_codes:
            log.info("  Nothing to do.")
            return

        # ── Step 2: Build SKU sets for classification ──────────────────────────
        sku_map = {}
        for code in rts_codes:
            qty_map = details_map.get(code, {}).get("qty_map", {})
            sku_map[code] = set(qty_map.keys()) if qty_map else set()

        # ── Step 3: Download label PDFs (concurrency=5) ────────────────────────
        log.info("Downloading label PDFs…")
        sem = asyncio.Semaphore(5)

        async def get_label(code):
            async with sem:
                data = await client._post(
                    "/services/rest/v1/oms/shippingPackage/getInvoiceLabel",
                    {"shippingPackageCode": code},
                )
                b64 = data.get("label") or data.get("invoiceLabel") or ""
                if b64:
                    return code, base64.b64decode(b64)
                link = data.get("shippingLabelLink") or data.get("invoiceLabelLink", "")
                if link:
                    return code, await client.get_label_pdf(link)
                return code, None

        label_results = await asyncio.gather(
            *[get_label(c) for c in rts_codes], return_exceptions=True
        )

        # ── Step 4: Classify and bucket ────────────────────────────────────────
        group_pdfs = {g: [] for g, _ in LABEL_GROUPS}
        no_label   = []

        for item in label_results:
            if isinstance(item, Exception):
                log.warning(f"  Error: {item}")
                continue
            code, pdf = item
            if not pdf:
                no_label.append(code)
                log.warning(f"  ⚠ No label for {code}")
                continue
            group = classify_shipment(sku_map.get(code, set()))
            group_pdfs[group].append(pdf)
            log.info(f"  ✓ {code} → {group}")

        # ── Step 5: Save merged PDFs ───────────────────────────────────────────
        log.info("Saving grouped PDFs…")
        prefix = channel.capitalize()
        saved_files = []
        for group_name, pdfs in group_pdfs.items():
            if pdfs:
                fname = f"{prefix}_Labels_{group_name}_{len(pdfs)}.pdf"
                save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / fname)
                log.info(f"  ✓ Saved: {fname} ({len(pdfs)} labels)")
                saved_files.append(OUTPUT_DIR / fname)

        # ── Step 6: Upload to Google Drive ─────────────────────────────────────
        log.info("Uploading to Google Drive…")
        uploader = GDriveUploader()
        folder_id = uploader.get_or_create_date_folder(DATE_STR)
        for fpath in saved_files:
            uploader.upload_file(fpath, folder_id)
            log.info(f"  ✓ Uploaded: {fpath.name}")

        # ── Summary ────────────────────────────────────────────────────────────
        total = sum(len(v) for v in group_pdfs.values())
        log.info(f"\n{'='*50}")
        log.info(f"  Channel: {channel}")
        log.info(f"  Total labels downloaded: {total} / {len(rts_codes)}")
        log.info(f"  Missing labels: {len(no_label)}")
        log.info(f"  PDFs saved: {len(saved_files)}")
        for g, pdfs in group_pdfs.items():
            if pdfs:
                log.info(f"    {g}: {len(pdfs)} labels")
        if no_label:
            log.warning(f"  Missing: {no_label}")
        log.info(f"{'='*50}")


asyncio.run(main())
