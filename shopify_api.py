"""
shopify_api.py — Shopify order cancellation
=============================================
Cancels unserviceable Shopify orders. Since these orders are unfulfilled,
Shopify automatically issues a full refund to the customer upon cancellation.

Required .env variables:
  SHOPIFY_STORE_URL   e.g. gimimichi.myshopify.com
  SHOPIFY_API_TOKEN   e.g. shpat_xxxxxxxxxxxxxxxxxxxx
"""

import logging
import os

import httpx

log = logging.getLogger("shopify_api")


class ShopifyClient:
    def __init__(self):
        store = os.environ.get("SHOPIFY_STORE_URL", "").rstrip("/")
        token = os.environ.get("SHOPIFY_API_TOKEN", "")
        if not store or not token:
            raise ValueError("SHOPIFY_STORE_URL and SHOPIFY_API_TOKEN must be set in .env")

        self._base    = f"https://{store}/admin/api/2024-01"
        self._headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type":           "application/json",
        }
        self._http = httpx.AsyncClient(headers=self._headers, timeout=30)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self._http.aclose()

    async def cancel_order(self, shopify_order_id: str) -> dict:
        """
        Cancel an unfulfilled Shopify order.
        Shopify automatically refunds the customer on cancellation.
        """
        url  = f"{self._base}/orders/{shopify_order_id}/cancel.json"
        body = {
            "reason": "other",
            "note":   "Auto-cancelled: pincode unserviceable at time of dispatch.",
            "restock": True,
            "notify":  True,  # Shopify sends cancellation + refund email to customer
        }
        resp = await self._http.post(url, json=body)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Shopify cancel failed for order {shopify_order_id}: "
                f"{resp.status_code} {resp.text[:300]}"
            )
        data = resp.json().get("order", {})
        log.info(f"  ✓ Order #{data.get('order_number')} cancelled — Shopify will auto-refund the customer")
        return data


async def cancel_order(shopify_order_id: str, display_order_code: str) -> bool:
    """
    Convenience wrapper: cancel a Shopify order.
    Returns True on success.
    """
    try:
        async with ShopifyClient() as client:
            await client.cancel_order(shopify_order_id)
            return True
    except Exception as e:
        log.error(f"  ✗ Cancel failed for order #{display_order_code}: {e}")
        return False
