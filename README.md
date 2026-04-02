# Gimi Michi — Order Automation

Automates daily order processing for Gimi Michi across three sales channels (Shopify, CRED, Flipkart) via the Unicommerce REST API. No browser, no UI clicks.

---

## What it does

For each channel, the script:
1. Fetches all `CREATED` shipments from Unicommerce
2. Creates invoices and generates shipping labels
3. Merges labels into split PDFs by SKU group
4. Creates a shipping manifest
5. Uploads all PDFs to Google Drive

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Create `.env` file
```
UNICOMMERCE_USERNAME=your@email.com
UNICOMMERCE_PASSWORD=yourpassword
UNICOMMERCE_FACILITY=FACILITY_CODE
GDRIVE_ROOT_FOLDER_ID=your_google_drive_folder_id
```

### 3. Authenticate Google Drive (one-time)
```bash
python3 auth.py
```
This opens a browser, asks you to log in with your Google account, and saves `token.json`. After this, Drive uploads work automatically.

---

## Daily automation — `main.py`

### Run the full daily script
```bash
python3 main.py                    # All channels: Shopify + CRED + Flipkart
python3 main.py --flipkart-only    # Flipkart only
python3 main.py --shopify-only     # Shopify only
python3 main.py --cred-only        # CRED only
python3 main.py --skip-flipkart    # Shopify + CRED only
python3 main.py --dry-run          # Count matched orders, make no changes
```

### Limit number of orders processed
```bash
python3 main.py --limit=5                                        # 5 per channel
python3 main.py --flipkart-only --flipkart-limit=10              # 10 Flipkart orders
python3 main.py --cred-limit=3 --shopify-limit=10 --flipkart-limit=5
```

### Flipkart TAT (dispatch deadline) filter
By default, Flipkart only processes orders whose `fulfillmentTat` (dispatch deadline) is **tomorrow**. If run between 12:00 AM–1:59 AM, it treats today as tomorrow (late-night run).

Override with explicit dates:
```bash
python3 main.py --flipkart-only --flipkart-start-date="3 April"
python3 main.py --flipkart-only --flipkart-start-date="1 April" --flipkart-end-date="5 April"
```
Date formats accepted: `3 April`, `2026-04-03`, `3rd April 2026`, etc.

### Filter by order date (all channels)
```bash
python3 main.py --date="2 April"
```

### Scheduled run
The script runs automatically at **10:00 PM daily** via a launchd job. To load/unload:
```bash
cp launchd/com.gimimichi.orders.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.gimimichi.orders.plist
launchctl unload ~/Library/LaunchAgents/com.gimimichi.orders.plist
```

---

## Label PDF groups

Both the daily script and the one-off tool split labels into these files:

| File suffix | SKUs included |
|---|---|
| `OGExpBox` | `Experience_Box_Normal`, `GMK06105` |
| `CurryChickenExpBox` | `Experience_Box_Curry_Chicken_Shopify`, `GMK04306` |
| `CheesyExpBox` | `Experience_Box_Cheesy_Shopify` |
| `VeggieExpBox` | `GMK05106` |
| `4Packs` | All 4-pack and 8-pack SKUs |
| `6Packs` | All 6-pack and 12-pack SKUs |

---

## One-off label fetcher — `fetch_rts_labels.py`

Use this to manually (re-)generate split label PDFs for any status and channel — for example, after labels are already generated but you need them re-split, or for orders in `MANIFESTED` status.

```bash
python3 fetch_rts_labels.py                                        # Shopify, READY_TO_SHIP
python3 fetch_rts_labels.py --channel=FLIPKART                     # Flipkart, READY_TO_SHIP
python3 fetch_rts_labels.py --channel=FLIPKART --status=MANIFESTED # Flipkart, MANIFESTED
```

Downloads labels, splits by SKU group, and uploads to the same Google Drive folder as the daily run.

---

## Other utilities

### `manual_split.py`
Manually split a specific shipment code into sub-packages.
```bash
python3 manual_split.py
```

### `cancel_unserviceable.py`
Cancel shipments flagged as unserviceable.
```bash
python3 cancel_unserviceable.py
```

### `check_shipment.py`
Check the status of a specific shipment code.
```bash
python3 check_shipment.py <shipment_code>
```

---

## Output files

All output goes to `output/YYYY-MM-DD/` and is also uploaded to Google Drive under a folder of the same date.

| File | Contents |
|---|---|
| `Flipkart_Invoices.pdf` | All Flipkart invoices merged |
| `Flipkart_Labels_<Group>_N.pdf` | Flipkart labels split by SKU group |
| `Shopify_Invoices.pdf` | All Shopify invoices merged |
| `Shopify_Labels_<Group>_N.pdf` | Shopify labels split by SKU group |
| `CRED_Invoices.pdf` | All CRED invoices merged |
| `CRED_Labels_<Courier>_N.pdf` | CRED labels split by courier |

---

## Project structure

```
main.py                  # Daily automation script (all channels)
unicommerce_api.py       # Unicommerce REST API client
fetch_rts_labels.py      # One-off: fetch + split labels for any status/channel
manual_split.py          # One-off: manually split a shipment
cancel_unserviceable.py  # One-off: cancel unserviceable shipments
check_shipment.py        # One-off: check a single shipment
shopify_api.py           # Shopify API client (used for auto-cancellation)
pdf_utils.py             # PDF merge/save helpers
gdrive.py                # Google Drive upload client
auth.py                  # One-time Google OAuth setup
.env                     # Credentials (not committed)
token.json               # Google OAuth token (not committed)
launchd/                 # macOS launchd config for scheduled runs
output/                  # Generated PDFs (by date)
logs/                    # Run logs (by date)
```
