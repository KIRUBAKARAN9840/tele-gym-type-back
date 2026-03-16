# app/utils/whatsapp/test_whatsapp.py
# Run this script to test WhatsApp template messaging

import asyncio
import os

# Set your credentials here for testing (or use environment variables)
os.environ["WHATSAPP_BASE_URL"] = "https://103.229.250.150"
os.environ["WHATSAPP_CLIENT_ID"] = "YOUR_CLIENT_ID"          # Replace with your client ID
os.environ["WHATSAPP_CLIENT_PASSWORD"] = "YOUR_PASSWORD"      # Replace with your password
os.environ["WHATSAPP_FROM_NUMBER"] = "91XXXXXXXXXX"           # Replace with your registered number

from whatsapp_client import WhatsAppClient, send_template, send_daily_pass


async def test_single_template():
    """Test sending a single template message"""
    print("\n--- Testing Single Template Message ---")

    response = await send_template(
        to="919876543210",                    # Replace with test phone number
        template_name="your_template_id",     # Replace with your template ID/name
        variables=["FitZone Gym", "199"]      # Replace with your template variables
    )

    print(f"Success: {response.success}")
    print(f"Status: {response.status_code} - {response.status_text}")
    print(f"Message ID: {response.message_id}")
    print(f"GUID: {response.guid}")
    if response.error:
        print(f"Error: {response.error}")


async def test_daily_pass():
    """Test sending daily pass promo"""
    print("\n--- Testing Daily Pass Template ---")

    response = await send_daily_pass(
        to="919876543210",                    # Replace with test phone number
        gym_name="FitZone Gym",
        price="199"
    )

    print(f"Success: {response.success}")
    print(f"Status: {response.status_code} - {response.status_text}")
    print(f"Message ID: {response.message_id}")
    if response.error:
        print(f"Error: {response.error}")


async def test_with_custom_client():
    """Test with custom client configuration"""
    print("\n--- Testing with Custom Client ---")

    client = WhatsAppClient(
        client_id="YOUR_CLIENT_ID",
        client_password="YOUR_PASSWORD",
        from_number="91XXXXXXXXXX",
        base_url="https://103.229.250.150"
    )

    response = await client.send_template(
        to="919876543210",
        template_name="your_template_id",
        variables=["Test Gym", "99"]
    )

    print(f"Success: {response.success}")
    print(f"Status: {response.status_code} - {response.status_text}")


async def main():
    print("=" * 50)
    print("WhatsApp Template Message Test")
    print("=" * 50)

    # Uncomment the test you want to run:

    await test_single_template()
    # await test_daily_pass()
    # await test_with_custom_client()


if __name__ == "__main__":
    asyncio.run(main())
