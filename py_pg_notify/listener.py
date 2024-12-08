from .pgmanager import PGManager


class Listener(PGManager):
    """
    A class for listening to PostgreSQL notifications.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listeners = {}

    async def add_listener(self, channel: str, callback):
        """
        Adds a listener for a specific channel.

        Args:
            channel (str): The channel to listen to.
            callback (callable): A function to handle notifications.

        Raises:
            RuntimeError: If called before the connection is established.
        """
        if self.conn is None:
            raise RuntimeError(
                "Listener not connected. Call `connect()` before adding a listener."
            )

        async def _wrapped_callback(connection, pid, channel, payload):
            await callback(connection, pid, channel, payload)

        self.listeners[channel] = _wrapped_callback
        await self.conn.add_listener(channel, _wrapped_callback)
        print(f"Listening on channel '{channel}'...")

    async def remove_listener(self, channel: str):
        """
        Removes the listener for a specific channel.

        Args:
            channel (str): The channel to stop listening to.
        """
        if channel in self.listeners:
            await self.conn.remove_listener(channel, self.listeners[channel])
            del self.listeners[channel]
            print(f"Stopped listening on channel '{channel}'.")

    async def close(self):
        """
        Closes the connection to the PostgreSQL database and removes all listeners.
        """
        if self.conn:
            for channel, callback in self.listeners.items():
                await self.conn.remove_listener(channel, callback)
            await self.conn.close()
            self.conn = None
            self.listeners = {}
            print("Listener connection closed.")
