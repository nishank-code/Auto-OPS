"""
Gimi Michi - Uniware Split Shipment Automation (REST API)
=========================================================
Reads your Uniware CSV export, computes which orders need splitting,
calls the Unicommerce REST API to split them, then generates a
label_print_order.html file grouping all shipments by SKU batch
so the warehouse manager can print labels in the correct order.

Setup:
  1. pip3 install requests python-dotenv
  2. Create a .env file in the same folder:
       UNIWARE_USERNAME=your_username
       UNIWARE_PASSWORD=your_password
       UNIWARE_FACILITY=your_facility_code
  3. Run: python3 split_shipments_api.py "your_export.csv"

Facility code: Uniware → Settings → Facility
"""

import os
import sys
import requests
from collections import Counter
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────────
UNIWARE_BASE  = "https://gimimichi.unicommerce.co.in"
USERNAME      = os.getenv("UNIWARE_USERNAME")
PASSWORD      = os.getenv("UNIWARE_PASSWORD")
FACILITY_CODE = os.getenv("UNIWARE_FACILITY")

# ─── SKU CLASSIFICATION ────────────────────────────────────────────────────
EXPERIENCE_BOXES = {
    # Legacy Shopify SKUs
    "Experience_Box_Normal_Shopify",           # Authentic Korean Experience Box
    "Broke_Memers_Experience_Box_Shopify",     # Broke Memers Experience Box
    "Experience_Box_Chicken_Shopify",          # Chicken Experience Box
    "Experience_Box_Cheesy_Shopify",           # Cheesy Experience Box
    "Experience_Box_Curry_Chicken_Shopify",    # Curry Chicken Experience Box
    # GMK experience box codes
    "GMK06105",                                # 5 Flavour Experience Box
    "GMK05106",                                # Kimchi 2 + Spicy 2 + Cheesy 2 Experience Box
    "GMK04306",                                # Hot Chicken 3 + Curry Chicken 3 Experience Box
}
FOUR_PACK_SKUS = {
    # Legacy Shopify SKUs
    "Hot_Kimchi-4", "Korean_Spicy-4", "Hot_Kimchi-2_and_Korean_Spicy-2",
    "Hot_Chicken-2_and_Curry_Chicken-2", "Hot_Chicken-4", "Crazy_Cheesy_4", "Curry_Chicken_4",
    # GMK 4-pack codes
    "GMK00104",    # Kimchi 4 Pack
    "GMK00204",    # Spicy 4 Pack
    "GMK00304",    # Hot Chicken 4 Pack
    "GMK00404",    # Curry Chicken 4 Pack
    "GMK00504",    # Cheesy 4 Pack
    "GMK01104",    # Kimchi 2 + Spicy 2 (4-pack)
    "GMK01204",    # Curry Chicken 2 + Hot Chicken 2 (4-pack)
    "GMS00301",    # Hot Chicken Pack of 4 (legacy)
    # GMK 8-pack codes — treated as 4-packs (effectively 2x 4-pack)
    "GMK00108",    # Kimchi 8 Pack
    "GMK00208",    # Spicy 8 Pack
}
EIGHT_PACK_SKUS = {
    "Hot_Kimchi-8", "Korean_Spicy-8",  # Legacy Shopify 8-packs (still alone per shipment)
}
SIX_PACK_SKUS   = {
    "Korean_Kimchi-2_Korean_Spicy-2_Crazy_Cheesy-2",  # Legacy
    "GMK02106",    # Veggie Trio Box: 2 Kimchi + 2 Spicy + 2 Cheesy (6-pack)
}
STICKER_SKUS    = {"Stickers", "GMS20002"}   # GMS20002 = Sticker Set
TOTE_SKUS       = {"Tote_Bag", "GMS20001"}   # GMS20001 = Daebak Tote Bag

# Sticker attachment priority (which experience box gets the sticker if multiple boxes in order)
# Old boxes take priority; among new GMK boxes: 5 Flavour first, then others equal
STICKER_PRIORITY = [
    "Experience_Box_Normal_Shopify",           # Authentic Korean
    "Broke_Memers_Experience_Box_Shopify",     # Broke Memers
    "Experience_Box_Cheesy_Shopify",           # Cheesy
    "Experience_Box_Curry_Chicken_Shopify",    # Curry Chicken
    "Experience_Box_Chicken_Shopify",          # Chicken
    "GMK06105",                                # 5 Flavour (1st among new)
    "GMK05106",                                # Kimchi+Spicy+Cheesy (equal 2nd)
    "GMK04306",                                # Chicken+Curry Chicken (equal 2nd)
]
TOTE_PRIORITY = [
    "Broke_Memers_Experience_Box_Shopify",     # Broke Memers
    "Experience_Box_Normal_Shopify",           # Authentic Korean
    "Experience_Box_Cheesy_Shopify",           # Cheesy
    "Experience_Box_Curry_Chicken_Shopify",    # Curry Chicken
    "Experience_Box_Chicken_Shopify",          # Chicken
    "GMK06105",                                # 5 Flavour (1st among new)
    "GMK05106",                                # Kimchi+Spicy+Cheesy (equal 2nd)
    "GMK04306",                                # Chicken+Curry Chicken (equal 2nd)
]

# ─── LABEL BATCH ORDER ─────────────────────────────────────────────────────
# Each entry: (batch display name, set of SKUs that belong to this batch)
# A shipment is assigned to the FIRST batch whose SKUs appear in the shipment.
LABEL_BATCHES = [
    ("Authentic Korean Experience Box",        {"Experience_Box_Normal_Shopify"}),
    ("Broke Memers Experience Box",            {"Broke_Memers_Experience_Box_Shopify"}),
    ("Cheesy Experience Box",                  {"Experience_Box_Cheesy_Shopify"}),
    ("Curry Chicken Experience Box",           {"Experience_Box_Curry_Chicken_Shopify"}),
    ("Chicken Experience Box",                 {"Experience_Box_Chicken_Shopify"}),
    ("5 Flavour Experience Box",               {"GMK06105"}),
    ("Kimchi+Spicy+Cheesy Experience Box",     {"GMK05106"}),
    ("Chicken+Curry Chicken Experience Box",   {"GMK04306"}),
    ("Pack-only Shipments",                    FOUR_PACK_SKUS | EIGHT_PACK_SKUS | SIX_PACK_SKUS),
    ("Other / Mixed",                          set()),  # catch-all
]

BATCH_COLORS = {
    "Authentic Korean Experience Box":       "#E8472A",
    "Broke Memers Experience Box":           "#6C3483",
    "Cheesy Experience Box":                 "#D4AC0D",
    "Curry Chicken Experience Box":          "#E67E22",
    "Chicken Experience Box":                "#E74C3C",
    "5 Flavour Experience Box":              "#1E8BC3",
    "Kimchi+Spicy+Cheesy Experience Box":    "#1ABC9C",
    "Chicken+Curry Chicken Experience Box":  "#E91E8C",
    "Pack-only Shipments":                   "#1A5276",
    "Other / Mixed":                         "#566573",
}


def get_batch(sku_set):
    """Return the batch name for a set of SKUs."""
    for name, batch_skus in LABEL_BATCHES:
        if not batch_skus:  # catch-all
            return name
        if sku_set & batch_skus:
            return name
    return "Other / Mixed"


# ─── SKU CLASSIFICATION ────────────────────────────────────────────────────
def classify(sku):
    if sku in EXPERIENCE_BOXES: return "experience_box"
    if sku in EIGHT_PACK_SKUS:  return "eight_pack"
    if sku in SIX_PACK_SKUS:    return "six_pack"
    if sku in FOUR_PACK_SKUS:   return "four_pack"
    if sku in STICKER_SKUS:     return "sticker"
    if sku in TOTE_SKUS:        return "tote"
    return "other"


# ─── SPLITTING ALGORITHM ───────────────────────────────────────────────────
# Rules:
#   1. Each experience box → its own shipment. Stickers/totes may attach to a
#      box shipment only, never to a pack shipment.
#   2. Pack shipments:
#      - Eight-pack: always alone (no sharing)
#      - Six-pack: may share with exactly 1 four-pack (different SKU only)
#      - Two DIFFERENT four-pack SKUs: may share one shipment
#      - Same four-pack SKU twice: must be separate shipments
#      - Max 2 four-packs per shipment; a six-pack counts as "using up" one slot
#   3. Stickers/totes attach to the highest-priority box in the order.
#      If no box exists, they attach to the first shipment.
#
def compute_split(sku_qty_map):
    units = []
    for sku, qty in sku_qty_map.items():
        for _ in range(qty):
            units.append(sku)

    exp_boxes = [s for s in units if classify(s) == "experience_box"]
    packs     = [s for s in units if classify(s) in ("four_pack", "six_pack", "eight_pack")]
    stickers  = [s for s in units if classify(s) == "sticker"]
    totes     = [s for s in units if classify(s) == "tote"]
    others    = [s for s in units if classify(s) == "other"]

    # Rule 1: one shipment per experience box
    shipments = [[box] for box in exp_boxes]

    # Rule 2: pack allocation
    pack_queue = list(packs)
    while pack_queue:
        shipment  = []
        has_six   = False
        four_skus = set()   # which 4-pack SKUs are already placed in this shipment
        remaining = []

        for p in pack_queue:
            kind = classify(p)

            if kind == "eight_pack":
                # Always alone — only accept if shipment is still empty
                if not shipment:
                    shipment.append(p)
                else:
                    remaining.append(p)

            elif kind == "six_pack":
                if not has_six and not shipment:
                    # Start shipment with the six-pack
                    shipment.append(p)
                    has_six = True
                elif not has_six and len(four_skus) == 1:
                    # One four-pack is already here; six-pack may join
                    shipment.append(p)
                    has_six = True
                else:
                    remaining.append(p)

            elif kind == "four_pack":
                duplicate       = p in four_skus
                eight_present   = any(classify(x) == "eight_pack" for x in shipment)
                fours_full      = len(four_skus) >= 2
                six_plus_four   = has_six and len(four_skus) >= 1  # six + 1 four = full

                if duplicate or eight_present or fours_full or six_plus_four:
                    remaining.append(p)
                else:
                    shipment.append(p)
                    four_skus.add(p)

            else:
                # Unknown pack type — place alone if empty, else defer
                if not shipment:
                    shipment.append(p)
                else:
                    remaining.append(p)

        if not shipment:
            # Safety valve: force the first item through
            shipment.append(pack_queue[0])
            remaining = pack_queue[1:]

        shipments.append(shipment)
        pack_queue = remaining

    # Rule 3: attach stickers/totes to highest-priority box shipment
    def attach_accessories(item_list, priority_list):
        for item in item_list:
            placed = False
            for p_sku in priority_list:
                for s in shipments:
                    if p_sku in s:
                        s.append(item)
                        placed = True
                        break
                if placed:
                    break
            if not placed:
                for s in shipments:
                    if any(classify(x) == "experience_box" for x in s):
                        s.append(item)
                        placed = True
                        break
            if not placed and shipments:
                shipments[0].append(item)

    attach_accessories(stickers, STICKER_PRIORITY)
    attach_accessories(totes,    TOTE_PRIORITY)

    # Unknown SKUs go into the first shipment
    for o in others:
        if shipments:
            shipments[0].append(o)

    clean = [s for s in shipments if s]
    if len(clean) <= 1:
        return None
    return [dict(Counter(s)) for s in clean]


# ─── CSV PARSER ────────────────────────────────────────────────────────────
def parse_csv_line(line):
    result, current, in_quotes = [], "", False
    for ch in line:
        if ch == '"':   in_quotes = not in_quotes
        elif ch == ',' and not in_quotes: result.append(current); current = ""
        else: current += ch
    result.append(current)
    return result


def load_orders(csv_path):
    with open(csv_path, encoding="utf-8") as f:
        lines = f.read().strip().split("\n")

    headers = [h.strip().strip('"') for h in parse_csv_line(lines[0])]

    def col(name):
        for i, h in enumerate(headers):
            if h.lower() == name.lower(): return i
        for i, h in enumerate(headers):
            if name.lower() in h.lower(): return i
        return -1

    order_id_idx = col("Display Order #")
    sku_idx      = col("Item Type SKUs")
    shipment_cols = [i for i, h in enumerate(headers) if h.strip().strip('"').lower() == "shipment"]
    shipment_idx  = shipment_cols[1] if len(shipment_cols) >= 2 else (shipment_cols[0] if shipment_cols else -1)

    orders = {}
    for line in lines[1:]:
        if not line.strip(): continue
        cols = parse_csv_line(line)
        if len(cols) <= max(shipment_idx, order_id_idx, sku_idx): continue

        shipment_code = cols[shipment_idx].strip()
        order_id      = cols[order_id_idx].strip()
        skus_raw      = cols[sku_idx].strip()

        if not shipment_code or not skus_raw: continue
        # Accept both old format (GIMI...) and new format (SP/HR/26-27/...)
        if not (shipment_code.startswith("GIMI") or shipment_code.startswith("SP/")): continue

        skus = [s.strip() for s in skus_raw.split(",") if s.strip()]
        orders[shipment_code] = {
            "order_id":      order_id,
            "shipment_code": shipment_code,
            "qty_map":       dict(Counter(skus)),
        }

    return orders


def orders_needing_split(orders):
    result = []
    for order in orders.values():
        shipments = compute_split(order["qty_map"])
        if shipments:
            result.append({**order, "shipments": shipments})
    return result


# ─── API CLIENT ────────────────────────────────────────────────────────────
def get_access_token():
    url = f"{UNIWARE_BASE}/oauth/token"
    params = {
        "grant_type": "password",
        "username":   USERNAME,
        "password":   PASSWORD,
        "client_id":  "my-trusted-client",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise ValueError(f"No access_token in response: {data}")
    print(f"✓ Authenticated (token: {token[:12]}...)")
    return token


def split_shipment(token, shipment_code, shipments):
    """
    Returns dict of {packet_number_str: new_shipment_code} on success, or None on failure.
    """
    url = f"{UNIWARE_BASE}/services/rest/v1/oms/shippingPackage/split"

    split_packages = []
    for packet_num, sku_qty in enumerate(shipments, start=1):
        items = [
            {"skuCode": sku, "quantity": qty, "saleOrderItemCodes": []}
            for sku, qty in sku_qty.items()
        ]
        split_packages.append({"packetNumber": packet_num, "items": items})

    payload = {
        "shippingPackageCode": shipment_code,
        "splitPackages":       split_packages,
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"bearer {token}",
        "Facility":      FACILITY_CODE,
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if resp.status_code == 200 and data.get("successful"):
        new_codes = data.get("splitNumberToShippingPackageCode", {})
        print(f"  ✓ Split successful → {new_codes}")
        return new_codes  # e.g. {"1": "GIMI2121100", "2": "GIMI2121101"} or {"1": "SP/HR/26-27/00123", "2": "SP/HR/26-27/00124"}
    else:
        errors = data.get("errors", [])
        msgs = [e.get("message", str(e)) for e in errors] if errors else [str(data)]
        print(f"  ✗ API error: {msgs}")
        return None


# ─── LABEL SHEET GENERATOR ─────────────────────────────────────────────────
def sku_display(sku_qty_map):
    """Human-readable summary of SKUs in a shipment."""
    parts = []
    for sku, qty in sku_qty_map.items():
        label = sku.replace("_Shopify", "").replace("_", " ")
        parts.append(f"{label}" if qty == 1 else f"{label} ×{qty}")
    return ", ".join(parts)


def generate_label_sheet(all_shipments, output_path, date_str):
    """
    all_shipments: list of {"shipment_code": str, "order_id": str, "sku_qty": dict}
    Groups by LABEL_BATCHES order and writes an HTML file.
    """
    # Group
    batches = {name: [] for name, _ in LABEL_BATCHES}
    for s in all_shipments:
        batch = get_batch(set(s["sku_qty"].keys()))
        batches[batch].append(s)

    # Build HTML
    batch_sections = ""
    total_printed = 0
    for name, _ in LABEL_BATCHES:
        items = batches[name]
        if not items:
            continue
        color = BATCH_COLORS.get(name, "#566573")
        rows = ""
        for idx, s in enumerate(items, 1):
            rows += f"""
            <tr>
                <td class="num">{idx}</td>
                <td class="code">{s['shipment_code']}</td>
                <td class="order">#{s['order_id']}</td>
                <td class="contents">{sku_display(s['sku_qty'])}</td>
                <td class="check"><span class="checkbox"></span></td>
            </tr>"""
        total_printed += len(items)
        batch_sections += f"""
        <div class="batch">
            <div class="batch-header" style="background:{color}">
                <span class="batch-name">{name}</span>
                <span class="batch-count">{len(items)} label{'s' if len(items)!=1 else ''}</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th style="width:40px">#</th>
                        <th>Shipment Code</th>
                        <th>Order</th>
                        <th>Contents</th>
                        <th style="width:60px">Done</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Gimi Michi — Label Print Order {date_str}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: Arial, sans-serif; background: #f4f4f4; padding: 24px; color: #1c1c1c; }}
  .page-header {{ background: #E8472A; color: white; padding: 20px 28px; border-radius: 8px; margin-bottom: 24px; display: flex; justify-content: space-between; align-items: center; }}
  .page-header h1 {{ font-size: 22px; }}
  .page-header .meta {{ font-size: 13px; opacity: 0.85; text-align: right; line-height: 1.6; }}
  .summary {{ display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }}
  .summary-card {{ background: white; border-radius: 6px; padding: 14px 20px; flex: 1; min-width: 140px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .summary-card .val {{ font-size: 28px; font-weight: bold; color: #E8472A; }}
  .summary-card .lbl {{ font-size: 12px; color: #888; margin-top: 2px; }}
  .instructions {{ background: #FFF8E1; border-left: 4px solid #F39C12; padding: 12px 16px; border-radius: 4px; margin-bottom: 24px; font-size: 13px; color: #555; line-height: 1.6; }}
  .instructions strong {{ color: #1c1c1c; }}
  .batch {{ background: white; border-radius: 8px; margin-bottom: 20px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .batch-header {{ color: white; padding: 12px 20px; display: flex; justify-content: space-between; align-items: center; }}
  .batch-name {{ font-size: 16px; font-weight: bold; }}
  .batch-count {{ font-size: 13px; opacity: 0.85; background: rgba(255,255,255,0.2); padding: 2px 10px; border-radius: 12px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead tr {{ background: #f9f9f9; }}
  th {{ padding: 9px 14px; text-align: left; font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid #eee; }}
  td {{ padding: 10px 14px; font-size: 13px; border-bottom: 1px solid #f0f0f0; vertical-align: middle; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #fafafa; }}
  td.num {{ color: #bbb; font-size: 12px; }}
  td.code {{ font-family: monospace; font-size: 13px; font-weight: bold; color: #1c1c1c; }}
  td.order {{ color: #888; }}
  td.contents {{ color: #555; }}
  .checkbox {{ display: inline-block; width: 20px; height: 20px; border: 2px solid #ddd; border-radius: 4px; cursor: pointer; }}
  @media print {{
    body {{ background: white; padding: 0; }}
    .instructions {{ display: none; }}
    .batch {{ box-shadow: none; border: 1px solid #eee; page-break-inside: avoid; }}
    .checkbox {{ border-color: #999; }}
  }}
</style>
</head>
<body>
  <div class="page-header">
    <h1>Label Print Order</h1>
    <div class="meta">Gimi Michi · {date_str}<br>{total_printed} total shipments</div>
  </div>

  <div class="summary">
    {"".join(f'<div class="summary-card"><div class="val">{len(batches[name])}</div><div class="lbl">{name}</div></div>' for name, _ in LABEL_BATCHES if batches[name])}
  </div>

  <div class="instructions">
    <strong>How to use:</strong> Print labels in Uniware batch by batch, working top to bottom.
    Filter by each batch name or process shipments in the order listed.
    Tick off each shipment code as you print it.
  </div>

  {batch_sections}
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n📋 Label print order saved → {output_path}")
    print(f"   Open in any browser. Print with Ctrl+P / Cmd+P.")


# ─── MAIN ──────────────────────────────────────────────────────────────────
def main(csv_path):
    if not all([USERNAME, PASSWORD, FACILITY_CODE]):
        missing = [n for n, v in [
            ("UNIWARE_USERNAME", USERNAME),
            ("UNIWARE_PASSWORD", PASSWORD),
            ("UNIWARE_FACILITY", FACILITY_CODE)] if not v]
        print(f"ERROR: Missing in .env: {', '.join(missing)}")
        sys.exit(1)

    print(f"Loading orders from: {csv_path}")
    orders = load_orders(csv_path)
    print(f"Total shipments in CSV: {len(orders)}")

    to_split = orders_needing_split(orders)
    print(f"Orders needing split: {len(to_split)}")

    if not to_split:
        print("Nothing to split.")
    else:
        print("\nOrders to split:")
        for o in to_split:
            print(f"  Order {o['order_id']} | Shipment {o['shipment_code']}")
            for i, sku_qty in enumerate(o["shipments"], 1):
                print(f"    Packet {i}: {sku_qty}")

        print("\nGetting access token...")
        try:
            token = get_access_token()
        except Exception as e:
            print(f"ERROR: Authentication failed: {e}")
            sys.exit(1)

        print(f"\nProcessing {len(to_split)} orders...\n")

    # Track all final shipments for the label sheet:
    # Start with all orders that don't need splitting
    split_codes = {o["shipment_code"] for o in to_split}
    all_shipments = []

    for code, order in orders.items():
        if code not in split_codes:
            all_shipments.append({
                "shipment_code": code,
                "order_id":      order["order_id"],
                "sku_qty":       order["qty_map"],
            })

    success, failed = 0, []

    for i, order in enumerate(to_split):
        print(f"[{i+1}/{len(to_split)}] Order {order['order_id']} ({order['shipment_code']})")
        try:
            new_codes = split_shipment(token, order["shipment_code"], order["shipments"])
            if new_codes:
                success += 1
                # Map each new shipment code to its SKU content
                for packet_num_str, new_code in new_codes.items():
                    packet_idx = int(packet_num_str) - 1
                    if packet_idx < len(order["shipments"]):
                        sku_qty = order["shipments"][packet_idx]
                    else:
                        sku_qty = {}
                    all_shipments.append({
                        "shipment_code": new_code,
                        "order_id":      order["order_id"],
                        "sku_qty":       sku_qty,
                    })
            else:
                failed.append(order["order_id"])
                # Add original shipment to label sheet anyway
                all_shipments.append({
                    "shipment_code": order["shipment_code"],
                    "order_id":      order["order_id"],
                    "sku_qty":       order["qty_map"],
                })
        except Exception as e:
            print(f"  ✗ Exception: {e}")
            failed.append(order["order_id"])

    if to_split:
        print(f"\n{'='*50}")
        print(f"Done. {success}/{len(to_split)} orders split successfully.")
        if failed:
            print(f"Failed orders (split manually in Uniware): {failed}")

    # Generate label sheet
    date_str = datetime.now().strftime("%d %b %Y")
    sheet_path = os.path.join(os.path.dirname(os.path.abspath(csv_path)), "label_print_order.html")
    generate_label_sheet(all_shipments, sheet_path, date_str)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python3 split_shipments_api.py "your_export.csv"')
        sys.exit(1)
    main(sys.argv[1])
