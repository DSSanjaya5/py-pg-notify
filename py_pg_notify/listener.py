import asyncpg


class Listener:
    """
    A class to listen to PostgreSQL notifications using asyncpg.

    Features:
        - Single connection setup for multiple listeners.
        - Custom notification handling via user-provided functions.
        - Dynamic listener addition and removal.
    """

    def __init__(
        self, 
        dsn: str = None, 
        *, 
        user: str = None, 
        password: str = None, 
        host: str = "localhost", 
        port: int = 5432, 
        dbname: str = None
    ):
        """
        Initializes the Listener class with the given PostgreSQL connection details.

        Args:
            dsn (str, optional): The Data Source Name for PostgreSQL connection. Defaults to None.
            user (str, optional): The database user. Required if `dsn` is not provided.
            password (str, optional): The user's password. Required if `dsn` is not provided.
            host (str, optional): The database host. Defaults to "localhost".
            port (int, optional): The database port. Defaults to 5432.
            dbname (str, optional): The database name. Required if `dsn` is not provided.

        Raises:
            ValueError: If `dsn` is not provided and required parameters (`user`, `password`, `dbname`) are missing.
        """
        if dsn:
            self.dsn = dsn
        else:
            if not all([user, password, dbname]):
                raise ValueError(
                    "When `dsn` is not provided, `user`, `password`, and `dbname` are required."
                )
            self.dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        self.conn = None
        self.listeners = {}

    async def connect(self):
        """
        Establishes a connection to the PostgreSQL database.

        Raises:
            asyncpg.exceptions.PostgresError: If the connection to the PostgreSQL database fails.
        """
        if self.conn is None:
            self.conn = await asyncpg.connect(self.dsn)
            print("Connected to the PostgreSQL database.")

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
            raise RuntimeError("Listener not connected. Call `connect()` before adding a listener.")

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

    async def __aenter__(self):
        """
        Asynchronous context entry point.
        Connects to the PostgreSQL database.

        Returns:
            Listener: The instance of the Listener class.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Asynchronous context exit point.
        Closes the connection to the PostgreSQL database.
        """
        await self.close()
