"""
One-off: download labels for all Shopify READY_TO_SHIP shipments,
group by SKU type, merge PDFs, and upload to Google Drive.
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

SHOPIFY_LABEL_GROUPS = [
    ("5FlavourExpBox",   ["GMK06105"]),
    ("3+3ChickenExpBox", ["GMK04306"]),
    ("VeggieExpBox",     ["GMK05106"]),
    ("4_Pack", [
        "GMK00104","GMK00204","GMK00304","GMK00404","GMK00504",
        "GMK01104","GMK01204","GMK00108","GMK00208","GMS00301",
    ]),
    ("6_Pack", ["GMK02106","GMK06205","GMK01306"]),
]

def classify_shipment(sku_set):
    for group_name, keywords in SHOPIFY_LABEL_GROUPS:
        if any(kw in sku_set for kw in keywords):
            return group_name
    return "5FlavourExpBox"


async def main():
    async with UnicommerceClient(
        username=os.environ["UNICOMMERCE_USERNAME"],
        password=os.environ["UNICOMMERCE_PASSWORD"],
        facility_code=os.environ["UNICOMMERCE_FACILITY"],
    ) as client:

        # ── Step 1: All RTS Shopify codes ─────────────────────────────────────
        log.info("Fetching READY_TO_SHIP Shopify shipments…")
        rts_codes, details_map = await client.get_all_codes_for_channel(
            "READY_TO_SHIP", channel="Shopify"
        )
        log.info(f"  {len(rts_codes)} shipments")

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
        group_pdfs = {g: [] for g, _ in SHOPIFY_LABEL_GROUPS}
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
        saved_files = []
        for group_name, pdfs in group_pdfs.items():
            if pdfs:
                fname = f"Shopify_Labels_{group_name}_{len(pdfs)}.pdf"
                save_pdf(merge_pdfs(pdfs), OUTPUT_DIR / fname)
                log.info(f"  ✓ Saved: {fname} ({len(pdfs)} labels)")
                saved_files.append(OUTPUT_DIR / fname)

        # ── Step 6: Upload to Google Drive ─────────────────────────────────────
        log.info("Uploading to Google Drive…")
        uploader = GDriveUploader()
        folder_id = uploader.get_or_create_dated_folder(DATE_STR)
        for fpath in saved_files:
            uploader.upload_file(fpath, folder_id)
            log.info(f"  ✓ Uploaded: {fpath.name}")

        # ── Summary ────────────────────────────────────────────────────────────
        total = sum(len(v) for v in group_pdfs.values())
        log.info(f"\n{'='*50}")
        log.info(f"  Total labels downloaded: {total} / {len(rts_codes)}")
        log.info(f"  Missing labels: {len(no_label)}")
        log.info(f"  PDFs uploaded: {len(saved_files)}")
        for g, pdfs in group_pdfs.items():
            if pdfs:
                log.info(f"    {g}: {len(pdfs)} labels")
        if no_label:
            log.warning(f"  Missing: {no_label}")
        log.info(f"{'='*50}")


asyncio.run(main())
