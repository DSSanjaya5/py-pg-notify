import asyncio
from dotenv import load_dotenv
from py_pg_notify import Notifier, PGConfig

load_dotenv()  # Set the environment variables

# Define the configuration using environment variables
config = PGConfig()


async def setup_inventory_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS inventory (
        product_id SERIAL PRIMARY KEY,
        product_name TEXT NOT NULL,
        warehouse_id INT NOT NULL,
        stock INT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    insert_data_query = """
    INSERT INTO inventory (product_name, warehouse_id, stock)
    VALUES
        ('Product A', 1, 100),
        ('Product B', 2, 50),
        ('Product C', 1, 200),
        ('Product D', 3, 0),
        ('Product E', 2, 150)
    ON CONFLICT DO NOTHING;
    """

    async with Notifier(config) as notifier:
        # Create the table
        await notifier.execute(create_table_query)
        print("Inventory table created successfully.")

        # Insert dummy data
        await notifier.execute(insert_data_query)
        print("Dummy data inserted successfully.")


# Run the script
if __name__ == "__main__":
    asyncio.run(setup_inventory_table())
