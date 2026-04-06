"""
Unicommerce REST API Client
============================
All HTTP calls in one place. Confirmed working endpoints:

  Auth:        GET  /oauth/token
  List:        POST /services/rest/v1/oms/shippingPackage/getShippingPackages
  Details:     POST /services/rest/v1/oms/shippingPackage/getShippingPackageDetails
  Inv+Label:   POST /services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel
  Invoice PDF: GET  /services/rest/v1/oms/shippingPackage/getInvoiceLabel
  Dispatch:    POST /services/rest/v1/oms/shippingPackage/dispatch
  Split:       POST /services/rest/v1/oms/shippingPackage/split
"""

import asyncio
import logging
import time
from typing import Optional

import httpx

log = logging.getLogger("api")

BASE_URL = "https://gimimichi.unicommerce.co.in"


class UnicommerceAPIError(Exception):
    pass


class UnicommerceClient:

    def __init__(self, username: str, password: str, facility_code: str):
        self.username      = username
        self.password      = password
        self.facility_code = facility_code
        self._token: Optional[str] = None
        self._token_ts: float = 0.0
        self._http: Optional[httpx.AsyncClient] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def __aenter__(self):
        self._http = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
        )
        await self._authenticate()
        return self

    async def __aexit__(self, *_):
        if self._http:
            await self._http.aclose()

    # ── Auth ───────────────────────────────────────────────────────────────────
    async def _authenticate(self):
        resp = await self._http.get("/oauth/token", params={
            "grant_type": "password",
            "username":   self.username,
            "password":   self.password,
            "client_id":  "my-trusted-client",
        })
        resp.raise_for_status()
        data = resp.json()
        self._token = data.get("access_token")
        if not self._token:
            raise UnicommerceAPIError(f"No access_token in response: {data}")
        self._token_ts = time.time()
        log.info(f"Authenticated ✓ (token: {self._token[:12]}…)")

    async def _ensure_token(self):
        if time.time() - self._token_ts > 5 * 3600:
            log.info("Token near expiry — re-authenticating…")
            await self._authenticate()

    def _headers(self) -> dict:
        return {
            "Authorization": f"bearer {self._token}",
            "Facility":      self.facility_code,
            "Content-Type":  "application/json",
        }

    # ── HTTP helpers ───────────────────────────────────────────────────────────
    async def _get(self, path: str, params: dict = None) -> dict:
        await self._ensure_token()
        resp = await self._http.get(path, params=params, headers=self._headers())
        if not resp.is_success:
            log.error(f"GET {path} → {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        data = resp.json()
        if not data.get("successful", True):
            raise UnicommerceAPIError(f"GET {path} failed: {data.get('errors')}")
        return data

    async def _get_bytes(self, path: str, params: dict = None) -> bytes:
        await self._ensure_token()
        resp = await self._http.get(path, params=params, headers=self._headers())
        resp.raise_for_status()
        return resp.content

    async def _post(self, path: str, body: dict, timeout: httpx.Timeout = None) -> dict:
        await self._ensure_token()
        kw = {"json": body, "headers": self._headers()}
        if timeout is not None:
            kw["timeout"] = timeout
        resp = await self._http.post(path, **kw)
        if not resp.is_success:
            log.error(f"POST {path} → {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        data = resp.json()
        if not data.get("successful", True):
            raise UnicommerceAPIError(f"POST {path} failed: {data.get('errors')}")
        return data

    async def _download(self, url: str) -> bytes:
        await self._ensure_token()
        resp = await self._http.get(url, headers=self._headers())
        resp.raise_for_status()
        return resp.content

    # ═══════════════════════════════════════════════════════════════════════════
    #  PUBLIC METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    # ── 1. Get all shipment codes for a status ─────────────────────────────────
    async def get_all_shipment_codes(self, status: str) -> list[str]:
        """
        Returns all shipment codes in the given status across all channels.
        The API has no channel filter — use get_all_codes_for_channel() for that.

        POST /services/rest/v1/oms/shippingPackage/getShippingPackages
        Body: { "statusCode": "CREATED" }
        """
        data = await self._post(
            "/services/rest/v1/oms/shippingPackage/getShippingPackages",
            {"statusCode": status},
        )
        return data.get("shippingPackages", [])

    # ── 2. Get details for a single shipment ───────────────────────────────────
    async def get_shipment_details(self, shipment_code: str) -> dict:
        """
        Returns channel, order ID, and SKU→qty map for one shipment.

        POST /services/rest/v1/oms/shippingPackage/getShippingPackageDetails
        Body: { "shippingPackageCode": "GIMI123" or "SP/HR/26-27/00123" }

        SKUs come from saleOrderItems[].itemSku — each entry is one unit,
        so we count occurrences to build the qty map.
        """
        data = await self._post(
            "/services/rest/v1/oms/shippingPackage/getShippingPackageDetails",
            {"shippingPackageCode": shipment_code},
        )
        dto = data.get("shippingPackageDetailDTO", {})

        qty_map = {}
        for item in dto.get("saleOrderItems", []):
            sku = item.get("itemSku", "")
            if sku:
                qty_map[sku] = qty_map.get(sku, 0) + 1

        order_details = dto.get("saleOrderDetails", {})

        # shippingMethodCode — take from first order item
        shipping_method = ""
        items = dto.get("saleOrderItems", [])
        if items:
            shipping_method = items[0].get("shippingMethodCode", "")

        return {
            "shipment_code":    shipment_code,
            "order_id":         order_details.get("displayOrderCode") or dto.get("saleOrderCode", shipment_code),
            "shopify_order_id": dto.get("saleOrderCode", ""),
            "channel":          order_details.get("channel", ""),
            "shipping_provider":dto.get("shippingProvider", ""),
            "shipping_courier": dto.get("shippingCourier", "") or dto.get("shippingCourierCode", ""),
            "shipping_method":  shipping_method,
            "qty_map":          qty_map,
            "order_date":       order_details.get("displayOrderDateTime", ""),
            "fulfillment_tat":  order_details.get("fulfillmentTat"),  # epoch ms; dispatch deadline
        }

    # ── 3a. Fetch ALL details in one pass (share across channels) ─────────────
    async def get_all_details(self, status: str, concurrency: int = 15) -> dict:
        """
        Fetch details for every shipment in `status` concurrently.
        Returns {shipment_code: detail_dict}.

        Call this once and pass the result to get_all_codes_for_channel() for
        each channel — avoids re-fetching the same 400+ details per channel.
        """
        all_codes = await self.get_all_shipment_codes(status)
        log.info(f"  {len(all_codes)} total {status} shipments — fetching all details…")

        sem = asyncio.Semaphore(concurrency)

        async def _fetch(code):
            async with sem:
                try:
                    return await self.get_shipment_details(code)
                except Exception as e:
                    log.warning(f"  Could not fetch details for {code}: {e}")
                    return None

        results = await asyncio.gather(*[_fetch(c) for c in all_codes])
        return {r["shipment_code"]: r for r in results if r}

    # ── 3b. Filter pre-fetched details to a single channel ────────────────────
    async def get_all_codes_for_channel(
        self,
        status: str,
        channel: str,
        concurrency: int = 15,
        all_details: dict = None,
    ) -> tuple[list[str], dict]:
        """
        Filter shipments in `status` to a single `channel`.
        If `all_details` is provided (pre-fetched), no HTTP calls are made.

        Returns (matching_codes, detail_map).
        """
        if all_details is None:
            all_details = await self.get_all_details(status, concurrency)

        matching_codes = []
        sku_map = {}
        for code, detail in all_details.items():
            if detail["channel"].lower() == channel.lower():
                matching_codes.append(code)
                sku_map[code] = detail

        log.info(f"  → {len(matching_codes)} shipments match channel '{channel}'")
        return matching_codes, sku_map

    # ── 4. Create invoice + shipping label (CREATED → READY_TO_SHIP) ──────────
    async def create_invoice_and_label(self, shipment_code: str, timeout: float = 42.0) -> dict:
        """
        Single call that creates the invoice, generates the label, and moves
        the shipment from CREATED to READY_TO_SHIP.

        Returns dict with at least: invoiceDisplayCode, shippingLabelLink

        POST /services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel
        Body: { "shippingPackageCode": "...", "generateUniwareShippingLabel": true }

        PREREQUISITE: Shipping provider AWB generation must be set to
        'List' or 'API' in Unicommerce → Settings → Shipping Providers.

        `timeout` defaults to 42 s (not the client-level 60 s) so that a hung
        courier API (e.g. Proship down) fails fast instead of blocking for a
        full minute per order.
        """
        data = await self._post(
            "/services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel",
            {
                "shippingPackageCode":          shipment_code,
                "generateUniwareShippingLabel": True,
            },
            timeout=httpx.Timeout(timeout, connect=15.0),
        )
        log.debug(f"  Invoice+Label: {shipment_code} → {data.get('invoiceDisplayCode')}")
        return data

    # ── 5. Fetch invoice PDF bytes ─────────────────────────────────────────────
    async def get_invoice_pdf(self, shipment_code: str) -> bytes:
        """
        Fetch invoice PDF for a shipment.
        POST /services/rest/v1/oms/shippingPackage/getInvoiceLabel
        Response contains base64-encoded PDF in 'label' or 'invoiceLabel' field.
        """
        import base64
        data = await self._post(
            "/services/rest/v1/oms/shippingPackage/getInvoiceLabel",
            {"shippingPackageCode": shipment_code},
        )
        # Try base64 field first
        b64 = data.get("label") or data.get("invoiceLabel") or data.get("invoicePdf", "")
        if b64:
            return base64.b64decode(b64)
        # Fallback to link
        link = data.get("shippingLabelLink") or data.get("invoiceLabelLink", "")
        if link:
            return await self._download(link)
        log.warning(f"  getInvoiceLabel: no PDF data in response for {shipment_code}")
        return b""

    # ── 6. Download label PDF from URL ────────────────────────────────────────
    async def get_label_pdf(self, label_url: str) -> bytes:
        """
        Download label from the shippingLabelLink URL.
        S3 presigned URLs are self-authenticating — must NOT send Authorization header.
        """
        if "s3.amazonaws.com" in label_url or "s3." in label_url:
            # Raw download without auth headers for S3 presigned URLs
            resp = await self._http.get(label_url)
            resp.raise_for_status()
            return resp.content
        return await self._download(label_url)

    # ── 8. Dispatch ────────────────────────────────────────────────────────────
    async def dispatch(self, shipment_code: str) -> bool:
        """
        Mark a READY_TO_SHIP shipment as DISPATCHED.

        POST /services/rest/v1/oms/shippingPackage/dispatch
        Body: { "shippingPackageCode": "..." }
        """
        try:
            await self._post(
                "/services/rest/v1/oms/shippingPackage/dispatch",
                {"shippingPackageCode": shipment_code},
            )
            return True
        except UnicommerceAPIError as e:
            log.warning(f"  Dispatch failed for {shipment_code}: {e}")
            return False

    async def dispatch_all(self, codes: list[str], concurrency: int = 5) -> tuple[list[str], list[str]]:
        """Dispatch a list of codes concurrently. Returns (succeeded, failed)."""
        sem = asyncio.Semaphore(concurrency)

        async def _one(code):
            async with sem:
                return code, await self.dispatch(code)

        results = await asyncio.gather(*[_one(c) for c in codes])
        return [c for c, ok in results if ok], [c for c, ok in results if not ok]

    # ── 8. Create and complete manifest (dispatch group) ──────────────────────
    async def create_and_complete_manifest(
        self,
        channel: str,
        shipping_provider_code: str,
        shipping_provider_name: str,
        shipping_method_code: str,
        shipment_codes: list[str],
        third_party_shipping: bool = True,
        is_aggregator: bool = False,
        shipping_courier: str = "",
        timeout: float = 300.0,
    ) -> dict:
        """
        Creates and closes a shipping manifest in one call.
        This is equivalent to the UI's Quick Dispatch which groups shipments
        into a manifest before marking them dispatched.

        POST /services/rest/v1/oms/shippingManifest/createclose

        For CRED:   third_party_shipping=True, provider = courier assigned by CRED
        For Shopify: third_party_shipping=False, provider=Proship, is_aggregator=True
        """
        body = {
            "channel":                    channel,
            "shippingProviderCode":       shipping_provider_code,
            "shippingProviderName":       shipping_provider_name,
            "shippingMethodCode":         shipping_method_code,
            "thirdPartyShipping":         third_party_shipping,
            "shippingPackageCodes":       shipment_codes,
            "shippingProviderIsAggregator": is_aggregator,
        }
        if shipping_courier:
            body["shippingCourier"] = shipping_courier

        data = await self._post(
            "/services/rest/v1/oms/shippingManifest/createclose", body,
            timeout=httpx.Timeout(timeout, connect=15.0),
        )
        manifest_code = data.get("shippingManifestCode", "")
        status = data.get("shippingManifestStatus") or {}
        failed = status.get("failedShippingPackages") or []
        if failed:
            log.warning(f"  Manifest {manifest_code}: {len(failed)} package(s) excluded:")
            for f in failed:
                log.warning(f"    {f.get('code')}: {f.get('failureReason')}")
        added = len(shipment_codes) - len(failed)
        log.info(f"  ✓ Manifest created: {manifest_code} ({added}/{len(shipment_codes)} shipments added)")
        failed_codes = [f.get("code") for f in failed if f.get("code")]
        return data, failed_codes

    # ── 9. Split shipment ──────────────────────────────────────────────────────
    async def split_shipment(self, shipment_code: str, shipments: list[dict]) -> dict:
        """
        Split a shipment into sub-packages.
        `shipments` is a list of {sku: qty} dicts (one per new package).

        POST /services/rest/v1/oms/shippingPackage/split
        """
        split_packages = []
        for packet_num, sku_qty in enumerate(shipments, start=1):
            items = [
                {"skuCode": sku, "quantity": qty, "saleOrderItemCodes": []}
                for sku, qty in sku_qty.items()
            ]
            split_packages.append({"packetNumber": packet_num, "items": items})

        data = await self._post(
            "/services/rest/v1/oms/shippingPackage/split",
            {
                "shippingPackageCode": shipment_code,
                "splitPackages":       split_packages,
            },
        )
        new_codes = data.get("splitNumberToShippingPackageCode", {})
        log.info(f"  Split {shipment_code} → {new_codes}")
        return new_codes
