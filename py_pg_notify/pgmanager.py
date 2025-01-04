import os
import asyncpg


class PGConfig:
    """
    A class to manage PostgreSQL connection parameters and generate the DSN string.

    Parameters can be provided via explicit arguments, a dictionary, or environment variables,
    in that order of priority.

    Attributes:
        dsn (str): The generated Data Source Name for the PostgreSQL connection.

    Usage:
        config = PGConfig(user="myuser", password="mypassword", dbname="mydb")
    """

    def __init__(
        self,
        dsn: str = None,
        config_dict=None,
        *,
        user: str = None,
        password: str = None,
        host: str = None,
        port: int = None,
        dbname: str = None,
    ):
        """
        Initializes the PGConfig class.

        Args:
            config_dict (dict, optional): A dictionary containing connection parameters.
            dsn (str, optional): The Data Source Name for PostgreSQL connection. Defaults to None.
            user (str, optional): The database user.
            password (str, optional): The user's password.
            host (str, optional): The database host.
            port (int, optional): The database port.
            dbname (str, optional): The database name.

        Priority of parameter resolution:
        1. Explicit parameters passed to this class.
        2. Values from `config_dict`.
        3. Environment variables (`PG_USER`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT`, `PG_DBNAME`).
        """
        # Extract from dictionary if provided
        config_dict = config_dict or {}
        self.dsn = dsn or self._generate_dsn(
            user or config_dict.get("user") or os.getenv("PG_USER"),
            password or config_dict.get("password") or os.getenv("PG_PASSWORD"),
            host or config_dict.get("host") or os.getenv("PG_HOST", "localhost"),
            port or config_dict.get("port") or os.getenv("PG_PORT", 5432),
            dbname or config_dict.get("dbname") or os.getenv("PG_DBNAME"),
        )

    def _generate_dsn(self, user, password, host, port, dbname):
        if not all([user, password, dbname]):
            raise ValueError(
                "When `dsn` is not provided, `user`, `password`, and `dbname` are required."
            )
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


class PGManager:
    """
    A base class to manage PostgreSQL connections using asyncpg.

    Features:
        - Handles connection setup and teardown.
        - Provides utility methods for executing queries.
        - Designed to be extended by specific classes (e.g., Listener, Notifier).
    """

    def __init__(self, config: PGConfig):
        """
        Initializes the PGManager class with a PGConfig object.

        Args:
            config (PGConfig): An instance of PGConfig containing connection parameters.
        """
        self.dsn = config.dsn
        self.conn = None

    async def connect(self):
        """
        Establishes a connection to the PostgreSQL database.

        Raises:
            asyncpg.exceptions.PostgresError: If the connection to the PostgreSQL database fails.
        """
        try:
            if self.conn is None:
                self.conn = await asyncpg.connect(self.dsn)
        except asyncpg.exceptions.PostgresError as e:
            raise RuntimeError(f"Failed to connect to the PostgreSQL database: {e}")

    async def execute(self, query: str, *args):
        """
        Executes a SQL query on the PostgreSQL database.

        Args:
            query (str): The SQL query to execute.
            *args: Parameters for the SQL query.

        Returns:
            The result of the query execution.

        Raises:
            RuntimeError: If the connection is not established.
            asyncpg.exceptions.PostgresError: If the query execution fails.
        """
        if self.conn is None:
            raise RuntimeError("Connection not established. Call `connect()` first.")
        try:
            return await self.conn.execute(query, *args)
        except asyncpg.exceptions.PostgresError as e:
            raise RuntimeError(f"Failed to execute query: {e}")

    async def close(self):
        """
        Closes the connection to the PostgreSQL database.

        Raises:
            asyncpg.exceptions.PostgresError: If closing the connection fails.
        """
        try:
            if self.conn:
                await self.conn.close()
                self.conn = None
        except asyncpg.exceptions.PostgresError as e:
            raise RuntimeError(f"Failed to close the connection: {e}")

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
