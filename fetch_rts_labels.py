"""
One-off tool: download shipping labels for shipments in a given status/channel,
split by SKU group, merge into separate PDFs, and upload to Google Drive.

This is NOT part of the daily automation — run it manually whenever you need
split label PDFs, e.g. after the main script runs or to recover labels for
orders already in MANIFESTED status.

Usage:
  python3 fetch_rts_labels.py                                                    # Shopify, READY_TO_SHIP
  python3 fetch_rts_labels.py --channel=FLIPKART                                 # Flipkart, READY_TO_SHIP
  python3 fetch_rts_labels.py --channel=FLIPKART --status=MANIFESTED             # Flipkart, MANIFESTED only
  python3 fetch_rts_labels.py --channel=FLIPKART --status=READY_TO_SHIP,MANIFESTED  # Both combined into one set of PDFs

Arguments:
  --channel=<name>      Channel to filter by. Options: Shopify, FLIPKART, CRED (default: Shopify)
  --status=<code(s)>    Comma-separated Unicommerce status codes (default: READY_TO_SHIP)
                        e.g. READY_TO_SHIP,MANIFESTED

Output files (saved to output/YYYY-MM-DD/ and uploaded to the same Drive folder):
  <Channel>_Labels_OGExpBox_N.pdf
  <Channel>_Labels_CurryChickenExpBox_N.pdf
  <Channel>_Labels_CheesyExpBox_N.pdf
  <Channel>_Labels_VeggieExpBox_N.pdf
  <Channel>_Labels_4Packs_N.pdf
  <Channel>_Labels_6Packs_N.pdf

SKU groups (same classification used by the daily automation):
  OGExpBox           — Experience_Box_Normal, GMK06105
  CurryChickenExpBox — Experience_Box_Curry_Chicken_Shopify, GMK04306
  CheesyExpBox       — Experience_Box_Cheesy_Shopify
  VeggieExpBox       — GMK05106
  4Packs             — All 4-pack and 8-pack SKUs
  6Packs             — All 6-pack and 12-pack SKUs
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
    channel  = "Shopify"
    statuses = ["READY_TO_SHIP"]
    for arg in sys.argv[1:]:
        if arg.startswith("--channel="):
            channel = arg.split("=", 1)[1]
        elif arg.startswith("--status="):
            statuses = [s.strip() for s in arg.split("=", 1)[1].split(",")]

    log.info(f"Channel: {channel}  |  Statuses: {', '.join(statuses)}")

    async with UnicommerceClient(
        username=os.environ["UNICOMMERCE_USERNAME"],
        password=os.environ["UNICOMMERCE_PASSWORD"],
        facility_code=os.environ["UNICOMMERCE_FACILITY"],
    ) as client:

        # ── Step 1: Collect codes across all requested statuses ────────────────
        all_codes   = []
        sku_map     = {}

        for status in statuses:
            log.info(f"Fetching {status} {channel} shipments…")
            codes, details_map = await client.get_all_codes_for_channel(
                status, channel=channel
            )
            log.info(f"  {len(codes)} shipments in {status}")
            for code in codes:
                if code not in sku_map:   # avoid duplicates if status overlap
                    all_codes.append(code)
                    qty_map = details_map.get(code, {}).get("qty_map", {})
                    sku_map[code] = set(qty_map.keys()) if qty_map else set()

        log.info(f"Total unique shipments: {len(all_codes)}")

        if not all_codes:
            log.info("  Nothing to do.")
            return

        # ── Step 2: Download shipping label PDFs (concurrency=5) ──────────────
        log.info("Downloading shipping label PDFs…")
        sem = asyncio.Semaphore(5)

        async def get_label(code):
            async with sem:
                # createInvoiceAndGenerateLabel returns successful=False for already-processed
                # orders but still populates shippingLabelLink with a fresh S3 URL.
                # Bypass _post (which raises on successful=False) and read the raw response.
                await client._ensure_token()
                resp = await client._http.post(
                    "/services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel",
                    json={"shippingPackageCode": code, "generateUniwareShippingLabel": True},
                    headers=client._headers(),
                )
                data = resp.json()
                link = data.get("shippingLabelLink", "")
                if link:
                    return code, await client.get_label_pdf(link)
                return code, None

        label_results = await asyncio.gather(
            *[get_label(c) for c in all_codes], return_exceptions=True
        )

        # ── Step 3: Classify and bucket ────────────────────────────────────────
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

        # ── Step 4: Save merged PDFs ───────────────────────────────────────────
        log.info("Saving grouped PDFs…")
        prefix = channel.capitalize()
        saved_files = []
        for group_name, pdfs in group_pdfs.items():
            if pdfs:
                fname = f"{prefix}_Labels_{group_name}_{len(pdfs)}.pdf"
                save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / fname)
                log.info(f"  ✓ Saved: {fname} ({len(pdfs)} labels)")
                saved_files.append(OUTPUT_DIR / fname)

        # ── Step 5: Upload to Google Drive ─────────────────────────────────────
        log.info("Uploading to Google Drive…")
        uploader = GDriveUploader(
            root_folder_id=os.environ["GDRIVE_ROOT_FOLDER_ID"],
            token_file=str(Path(__file__).parent / "token.json"),
        )
        folder_id = uploader.get_or_create_date_folder(DATE_STR)
        for fpath in saved_files:
            uploader.upload_file(fpath, folder_id)
            log.info(f"  ✓ Uploaded: {fpath.name}")

        # ── Summary ────────────────────────────────────────────────────────────
        total = sum(len(v) for v in group_pdfs.values())
        log.info(f"\n{'='*50}")
        log.info(f"  Channel: {channel}  |  Statuses: {', '.join(statuses)}")
        log.info(f"  Total labels downloaded: {total} / {len(all_codes)}")
        log.info(f"  Missing labels: {len(no_label)}")
        log.info(f"  PDFs saved: {len(saved_files)}")
        for g, pdfs in group_pdfs.items():
            if pdfs:
                log.info(f"    {g}: {len(pdfs)} labels")
        if no_label:
            log.warning(f"  Missing: {no_label}")
        log.info(f"{'='*50}")


asyncio.run(main())
