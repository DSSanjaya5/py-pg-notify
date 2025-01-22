import asyncio
from datetime import datetime
import random
from dotenv import load_dotenv
from py_pg_notify import Notifier, PGConfig

load_dotenv()  # Set the environment variables


async def setup_notifier():
    # Define the configuration using environment variables
    config = PGConfig()

    async with Notifier(config) as notifier:

        await notifier.remove_trigger_function("inventory_notify_function")

        # Create a trigger function to notify on the 'inventory_update_channel'
        await notifier.create_trigger_function(
            "inventory_notify_function", "inventory_update_channel"
        )

        # Create a trigger for UPDATE events on the 'inventory' table
        await notifier.create_trigger(
            table_name="inventory",
            trigger_name="inventory_update_trigger",
            function_name="inventory_notify_function",
            event="UPDATE",
        )


async def simulate_random_stock_updates():
    """
    Generates random stock updates for products over a period of time.
    Ensures stock levels do not go negative.
    """
    # Define the configuration using environment variables
    config = PGConfig()

    async with Notifier(config) as notifier:
        print(f"[{datetime.now()}] Generating random stock updates...")
        try:
            while True:
                # Generate random product_id and stock change value
                product_id = random.randint(1, 5)
                stock_change = random.randint(
                    -20, 10
                )  # Random increase or decrease in stock

                # Fetch current stock for the product
                get_stock_query = f"""
                SELECT stock
                FROM inventory
                WHERE product_id = {product_id};
                """
                result = await notifier.execute(get_stock_query)
                current_stock = dict(result[0]).get("stock", 0)

                # Ensure stock does not go negative
                if stock_change < 0 and current_stock + stock_change < 0:
                    print(
                        f"[{datetime.now()}] Skipped update: Product ID {product_id} stock would go negative."
                    )
                else:
                    # Update the inventory table
                    stock_update_query = f"""
                    UPDATE inventory
                    SET stock = stock + {stock_change}
                    WHERE product_id = {product_id};
                    """
                    await notifier.execute(stock_update_query)
                    print(
                        f"[{datetime.now()}] Stock updated for Product ID {product_id}: {stock_change:+} units. Current Stock: {current_stock + stock_change}"
                    )

                # Wait for a random interval between updates
                await asyncio.sleep(random.uniform(1, 5))
        except asyncio.CancelledError:
            print(f"[{datetime.now()}] Random stock updates stopped.")


if __name__ == "__main__":
    try:
        # Run setup and start generating updates
        asyncio.run(setup_notifier())
        asyncio.run(simulate_random_stock_updates())
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Process terminated by user.")
    except Exception as e:
        print(f"Notifier Error: {e}")
