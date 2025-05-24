import asyncio
import json
from dotenv import load_dotenv
from py_pg_notify import Listener, Notification, PGConfig

load_dotenv()  # Set the environment variables


async def notification_handler(msg: Notification):
    """
    Processes the received notification with a detailed payload.
    """
    try:
        # Parse the JSON payload
        payload = json.loads(msg.payload)

        trigger_name = payload.get("trigger")
        event = payload.get("event")
        timing = payload.get("timing")
        new_data = payload.get("new", {})
        old_data = payload.get("old", {})

        product_id = new_data.get("product_id")
        product_name = new_data.get("product_name")
        warehouse_id = new_data.get("warehouse_id")
        new_stock = new_data.get("stock")
        old_stock = old_data.get("stock")

        # Check stock threshold and send alerts if necessary
        if new_stock < 20:
            print(
                f"⚠️ Alert: Stock for Product ID {product_id} ('{product_name}') is critically low! Notify warehouse staff immediately."
            )
        else:
            print(
                f"✅ Stock levels for Product ID {product_id} ('{product_name}') are sufficient."
            )

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON payload: {e}")
    except Exception as e:
        print(f"Error processing notification: {e}")


async def start_listener():
    # Define the configuration using environment variables
    config = PGConfig()

    async with Listener(config) as listener:
        # Add a listener for the 'inventory_update_channel'
        await listener.add_listener("inventory_update_channel", notification_handler)

        print(
            "Listener is active and waiting for inventory updates. Press Ctrl+C to exit."
        )
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(start_listener())
    except KeyboardInterrupt:
        print("Listener terminated.")
