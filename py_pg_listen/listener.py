import asyncio
import asyncpg


class Listener:
    """
    A class to listen to PostgreSQL notifications using asyncpg.

    The `Listener` class allows seamless integration with PostgreSQL's 
    `LISTEN` functionality. It supports both context manager usage 
    (for automatic resource cleanup) and manual connection control.

    Features:
        - Flexible connection setup using either a DSN string or individual parameters.
        - Asynchronous notification handling.
        - Robust error handling and resource cleanup.

    Attributes:
        channel (str): The PostgreSQL channel to listen to for notifications.
        dsn (str): The Data Source Name for connecting to PostgreSQL, either passed directly or constructed from other parameters.
        conn (asyncpg.Connection, optional): The active database connection. Initialized after calling `connect()`.
    """
    
    def __init__(
        self, 
        channel: str, 
        dsn: str = None, 
        *, 
        user: str = None, 
        password: str = None, 
        host: str = "localhost", 
        port: int = 5432, 
        dbname: str = None
    ):
        """
        Initializes the Listener class with the given PostgreSQL connection details and channel.

        Args:
            channel (str): The name of the channel to listen to.
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
            # Validate required parameters for constructing the DSN
            if not all([user, password, dbname]):
                raise ValueError(
                    "When `dsn` is not provided, `user`, `password`, and `dbname` are required."
                )
            self.dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        self.channel = channel
        self.conn = None
        self._stop_event = asyncio.Event()

    async def connect(self):
        """
        Establishes a connection to the PostgreSQL database and sets up the listener.

        Raises:
            asyncpg.exceptions.PostgresError: If the connection to the PostgreSQL database fails.
        """
        self.conn = await asyncpg.connect(self.dsn)
        await self.conn.add_listener(self.channel, self._on_notify)
        print(f"Listening on channel '{self.channel}'...")

    async def _on_notify(self, connection, pid, channel, payload):
        """
        Handles incoming notifications from PostgreSQL.

        Args:
            connection: The connection object.
            pid (int): The process ID that sent the notification.
            channel (str): The channel the notification was sent on.
            payload (str): The payload of the notification.
        """
        print(f"Received notification: Channel={channel}, Payload={payload}")

    async def start_listening(self):
        """
        Starts listening for notifications. This method blocks until `stop()` is called.

        Raises:
            RuntimeError: If called before the listener is connected.
        """
        if not self.conn:
            raise RuntimeError("Listener not connected. Call `connect()` before listening.")
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            pass  # Gracefully handle task cancellation.

    async def stop(self):
        """
        Stops listening for notifications.
        """
        self._stop_event.set()

    async def close(self):
        """
        Closes the connection to the PostgreSQL database.
        """
        if self.conn:
            await self.conn.remove_listener(self.channel, self._on_notify)
            await self.conn.close()
            self.conn = None
            print("Listener connection closed.")

    async def __aenter__(self):
        """
        Asynchronous context entry point.
        Connects to the PostgreSQL database and sets up the listener.

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
