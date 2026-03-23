import asyncio, os
from dotenv import load_dotenv
load_dotenv()

async def check():
    from unicommerce_api import UnicommerceClient
    async with UnicommerceClient(
        os.environ["UNICOMMERCE_USERNAME"],
        os.environ["UNICOMMERCE_PASSWORD"],
        os.environ["UNICOMMERCE_FACILITY"],
    ) as client:
        data = await client._post(
            "/services/rest/v1/oms/shippingPackage/getShippingPackageDetails",
            {"shippingPackageCode": "GIMI2126762"}
        )
        dto = data.get("shippingPackageDetailDTO", {})
        order = dto.get("saleOrderDetails", {})
        print("displayOrderCode:", order.get("displayOrderCode"))
        print("saleOrderCode:   ", dto.get("saleOrderCode"))
        print("channel:         ", order.get("channel"))
        print("All saleOrderDetails keys:", list(order.keys()))

asyncio.run(check())
