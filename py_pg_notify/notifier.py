"""
Module to manage PostgreSQL notification triggers using asyncpg.
"""

from .pgmanager import PGManager
from .utils import (
    create_trigger_function_query,
    GET_TRIGGER_FUNCTIONS_QUERY,
    GET_TRIGGERS_QUERY,
    drop_function_query,
    create_trigger_query,
    drop_trigger_query,
)


class Notifier(PGManager):
    """
    A class to manage PostgreSQL notification triggers using asyncpg.

    Key Features:
    - Connection management for PostgreSQL.
    - Dynamic creation and removal of triggers and notification functions.
    - Retrieval of existing triggers and functions.
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
        """
        super().__init__(
            dsn=dsn, user=user, password=password, host=host, port=port, dbname=dbname
        )

    async def create_trigger_function(self, function_name: str, channel: str):
        """
        Creates a PostgreSQL notification function.

        Args:
            function_name (str): The name of the trigger function to create.
            channel (str): The notification channel to send messages to.

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.
        """
        if self.conn is None:
            raise RuntimeError(
                "Notifier not connected. Call `connect()` before creating a function."
            )

        try:
            query = create_trigger_function_query(function_name, channel)
            await self.conn.execute(query)
        except Exception as e:
            raise Exception(f"Error creating trigger function {function_name}: {e}")

    async def get_trigger_functions(self, table_name: str, trigger_name: str = None):
        """
        Retrieves the trigger functions associated with a specific table and trigger.

        Args:
            table_name (str): The name of the table whose trigger functions to retrieve.
            trigger_name (str, optional): The name of a specific trigger. If None, all trigger functions for the table are returned.

        Returns:
            list: A list of function names associated with the specified table and trigger.

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        try:
            query = GET_TRIGGER_FUNCTIONS_QUERY
            params = [table_name]

            if trigger_name:
                query += " AND tgname = $2"
                params.append(trigger_name)

            rows = await self.conn.fetch(query, *params)
            return [row["function_name"] for row in rows]
        except Exception as e:
            raise Exception(
                f"Error retrieving trigger functions for table {table_name}: {e}"
            )

    async def remove_trigger_function(self, function_name: str):
        """
        Removes a specific trigger function and its associated triggers.

        Args:
            function_name (str): The name of the trigger function to remove.

        Returns:
            dict: A dictionary containing the function name and success status.

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        try:
            query = drop_function_query(function_name)
            await self.conn.execute(query)
            return {"function_name": function_name, "success": True}
        except Exception as e:
            raise Exception(f"Error removing trigger function {function_name}: {e}")

    async def create_trigger(
        self,
        table_name: str,
        trigger_name: str,
        function_name: str,
        event: str,
        timing: str = "AFTER",
    ):
        """
        Creates a PostgreSQL trigger for the specified table.

        Args:
            table_name (str): The name of the table to attach the trigger to.
            trigger_name (str): The name of the trigger to create.
            function_name (str): The name of the function to be executed when the trigger fires.
            event (str): The event that fires the trigger (e.g., 'INSERT', 'UPDATE', 'DELETE').
            timing (str, optional): The timing of the trigger ('BEFORE' or 'AFTER'). Defaults to "AFTER".

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.
        """
        if self.conn is None:
            raise RuntimeError(
                "Notifier not connected. Call `connect()` before creating a trigger."
            )

        try:
            query = create_trigger_query(
                table_name, trigger_name, function_name, event, timing
            )
            await self.conn.execute(query)
        except Exception as e:
            raise Exception(
                f"Error creating trigger {trigger_name} for table {table_name}: {e}"
            )

    async def get_triggers(self, table_name: str):
        """
        Retrieves all triggers associated with a specific table.

        Args:
            table_name (str): The name of the table whose triggers to retrieve.

        Returns:
            list: A list of trigger names associated with the specified table.

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        try:
            query = GET_TRIGGERS_QUERY
            rows = await self.conn.fetch(query, table_name)
            return [row["trigger_name"] for row in rows]
        except Exception as e:
            raise Exception(f"Error retrieving triggers for table {table_name}: {e}")

    async def remove_trigger(self, table_name: str, trigger_name: str):
        """
        Removes a trigger from the specified table.

        Args:
            table_name (str): The name of the table from which to remove the trigger.
            trigger_name (str): The name of the trigger to remove.

        Returns:
            dict: A dictionary containing the trigger name and success status.

        Raises:
            RuntimeError: If the connection to PostgreSQL is not established.

        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        try:
            query = drop_trigger_query(trigger_name, table_name)
            await self.conn.execute(query)
            return {"trigger_name": trigger_name, "success": True}
        except Exception as e:
            raise Exception(
                f"Error removing trigger {trigger_name} from table {table_name}: {e}"
            )
