import asyncpg


class PGManager:
    """
    A base class to manage PostgreSQL connections using asyncpg.

    Features:
        - Handles connection setup and teardown.
        - Provides utility methods for executing queries.
        - Designed to be extended by specific classes (e.g., Listener, Notifier).
    """

    def __init__(
        self,
        dsn: str = None,
        *,
        user: str = None,
        password: str = None,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = None,
    ):
        """
        Initializes the PGManager class.

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

    async def connect(self):
        """
        Establishes a connection to the PostgreSQL database.

        Raises:
            asyncpg.exceptions.PostgresError: If the connection to the PostgreSQL database fails.
        """
        if self.conn is None:
            self.conn = await asyncpg.connect(self.dsn)
            print("Connected to the PostgreSQL database.")

    async def execute(self, query: str, *args):
        """
        Executes a SQL query on the PostgreSQL database.

        Args:
            query (str): The SQL query to execute.
            *args: Parameters for the SQL query.

        Returns:
            The result of the query execution.
        """
        if self.conn is None:
            raise RuntimeError("Connection not established. Call `connect()` first.")
        return await self.conn.execute(query, *args)

    async def close(self):
        """
        Closes the connection to the PostgreSQL database.
        """
        if self.conn:
            await self.conn.close()
            self.conn = None
            print("Connection closed.")

    async def __aenter__(self):
        """
        Asynchronous context entry point.
        Connects to the PostgreSQL database.

        Returns:
            PGManager: The instance of the class.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Asynchronous context exit point.
        Closes the connection to the PostgreSQL database.
        """
        await self.close()
