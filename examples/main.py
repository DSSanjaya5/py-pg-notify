from py_pg_notify import Listener
import asyncio


async def handle_notification(connection, pid, channel, payload):
    print(f"Notification received: Channel={channel}, Payload={payload}")


async def context_manager_main():
    listener = Listener(user="postgres", password="1234", dbname="pg_test")
    async with listener:
        await listener.add_listener("ch_01", handle_notification)
        # Keep running
        await asyncio.sleep(3600)  # Simulate long-running process


async def explicit_cleanup_main():
    listener = Listener(user="postgres", password="1234", dbname="pg_test")
    await listener.connect()  # Explicitly connect to the database

    await listener.add_listener("ch_01", handle_notification)

    try:
        await asyncio.sleep(3600)  # Keep running
    finally:
        await listener.close()  # Explicitly close the connection


async def public_handler(connection, pid, channel, payload):
    print(f"Public Notification: {payload}")


async def group_handler(connection, pid, channel, payload):
    print(f"Group Notification: {payload}")


async def multiple_listeners_main():
    listener = Listener(user="postgres", password="1234", dbname="pg_test")
    async with listener:
        # Add listeners for multiple channels
        await listener.add_listener("ch_01", public_handler)
        await listener.add_listener("grp", group_handler)

        # Keep running
        await asyncio.sleep(3600)


async def dynamic_handler(connection, pid, channel, payload):
    print(f"Dynamic Notification: {payload}")


async def dynamic_add_remove_main():
    listener = Listener(user="postgres", password="1234", dbname="pg_test")
    async with listener:
        # Add a listener dynamically
        await listener.add_listener("ch_01", dynamic_handler)

        # Simulate runtime removal of a listener
        await asyncio.sleep(30)  # Wait for some time
        await listener.remove_listener("ch_01")
        print("Listener removed.")

        # Keep running
        await asyncio.sleep(3600)


# Run the listener
asyncio.run(dynamic_add_remove_main())
