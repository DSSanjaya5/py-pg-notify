import asyncpg
from pgmanager import PGManager


class Notifier(PGManager):
    """
    A class to manage PostgreSQL notification triggers using asyncpg.

    Features:
        - Single connection for managing triggers.
        - Dynamic creation of triggers and notification functions.
        - Context manager support for easier resource management.
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
        Initializes the Notifier class with the given PostgreSQL connection details.

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
        super().__init__(dsn=dsn, user=user, password=password, host=host, port=port, dbname=dbname)

    async def create_notification_function(self, function_name: str, channel: str, event: str):
        """
        Creates a PostgreSQL notification function.

        Args:
            function_name (str): The name of the notification function.
            channel (str): The channel for sending notifications.
            event (str): The event type (e.g., 'INSERT', 'UPDATE', 'DELETE').

        Raises:
            RuntimeError: If the connection is not established.
            ValueError: If invalid event type is provided.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` before creating a function.")

        valid_events = ['INSERT', 'UPDATE', 'DELETE']
        if event not in valid_events:
            raise ValueError(f"Invalid event type '{event}'. Valid types are: {', '.join(valid_events)}.")
        
        row_reference = 'NEW' if event != 'DELETE' else 'OLD'

        query = f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('{channel}', json_build_object('event', '{event}', 'row', {row_reference})::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        await self.conn.execute(query)
        print(f"Notification function '{function_name}' created for event '{event}'.")


    async def create_trigger(self, table_name: str, trigger_name: str, function_name: str, event: str, timing: str = 'AFTER'):
        """
        Creates a PostgreSQL trigger for the specified table.

        Args:
            table_name (str): The table to attach the trigger to.
            trigger_name (str): The name of the trigger.
            function_name (str): The name of the function to call.
            event (str): The event type (INSERT, UPDATE, DELETE, etc.).
            timing (str, optional): The timing of the trigger (BEFORE, AFTER). Defaults to 'AFTER'.

        Raises:
            RuntimeError: If the connection is not established.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` before creating a trigger.")

        query = f"""
        CREATE TRIGGER {trigger_name}
        {timing} {event} ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION {function_name}();
        """
        await self.conn.execute(query)
        print(f"Trigger '{trigger_name}' created on table '{table_name}' for event '{event}' with '{timing}' timing.")

    async def remove_trigger(self, table_name: str, trigger_name: str):
        """
        Removes a trigger from the specified table.

        Args:
            table_name (str): The table to remove the trigger from.
            trigger_name (str): The name of the trigger to remove.
        """
        query = f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};"
        await self.conn.execute(query)
        print(f"Trigger '{trigger_name}' removed from table '{table_name}'.")

    async def remove_function(self, function_name: str):
        """
        Removes a PostgreSQL function.

        Args:
            function_name (str): The name of the function to remove.
        """
        query = f"DROP FUNCTION IF EXISTS {function_name} CASCADE;"
        await self.conn.execute(query)
        print(f"Function '{function_name}' removed.")

    async def close(self):
        """
        Closes the connection to the PostgreSQL database.
        """
        await super().close()

    async def __aenter__(self):
        """
        Asynchronous context entry point.
        Connects to the PostgreSQL database.

        Returns:
            Notifier: The instance of the Notifier class.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Asynchronous context exit point.
        Closes the connection to the PostgreSQL database.
        """
        await self.close()
