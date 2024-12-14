"""
Module to manage PostgreSQL notification listeners using asyncpg.
"""

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
            Exception: If there is an error while adding the listener.
        """
        if self.conn is None:
            raise RuntimeError(
                "Listener not connected. Call `connect()` before adding a listener."
            )

        try:

            async def _wrapped_callback(connection, pid, channel, payload):
                await callback(connection, pid, channel, payload)

            self.listeners[channel] = _wrapped_callback
            await self.conn.add_listener(channel, _wrapped_callback)
        except Exception as e:
            raise Exception(f"Error adding listener to channel '{channel}': {e}")

    async def remove_listener(self, channel: str):
        """
        Removes the listener for a specific channel.

        Args:
            channel (str): The channel to stop listening to.

        Raises:
            RuntimeError: If called before the connection is established.
            KeyError: If no listener exists for the specified channel.
            Exception: If there is an error while removing the listener.
        """
        if self.conn is None:
            raise RuntimeError(
                "Listener not connected. Call `connect()` before removing a listener."
            )

        try:
            if channel in self.listeners:
                await self.conn.remove_listener(channel, self.listeners[channel])
                del self.listeners[channel]
            else:
                raise KeyError(f"No listener found for channel '{channel}'.")
        except KeyError as e:
            raise e
        except Exception as e:
            raise Exception(f"Error removing listener from channel '{channel}': {e}")

    async def close(self):
        """
        Closes the connection to the PostgreSQL database and removes all listeners.

        Raises:
            Exception: If there is an error while closing the connection or removing listeners.
        """
        if self.conn:
            try:
                for channel, callback in self.listeners.items():
                    await self.conn.remove_listener(channel, callback)
                await self.conn.close()
                self.conn = None
                self.listeners = {}
            except Exception as e:
                raise Exception(f"Error closing listener connection: {e}")
