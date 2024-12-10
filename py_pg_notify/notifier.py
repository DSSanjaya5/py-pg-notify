from .pgmanager import PGManager


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
        """
        super().__init__(
            dsn=dsn, user=user, password=password, host=host, port=port, dbname=dbname
        )

    async def create_trigger_function(self, function_name: str, channel: str):
        """
        Creates a PostgreSQL notification function.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` before creating a function.")

        query = f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify(
                '{channel}',
                json_build_object(
                    'trigger', TG_NAME,
                    'timing', TG_WHEN,
                    'event', TG_OP,
                    'new', NEW,
                    'old', OLD
                )::text
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        await self.conn.execute(query)

    async def get_trigger_functions(self, table_name: str, trigger_name: str = None):
        """
        Retrieves the trigger functions associated with a specific table and trigger.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        query = """
        SELECT pg_proc.proname AS function_name
        FROM pg_trigger
        INNER JOIN pg_class ON pg_class.oid = tgrelid
        INNER JOIN pg_proc ON pg_proc.oid = tgfoid
        WHERE pg_class.relname = $1
          AND NOT tgisinternal
        """
        params = [table_name]

        if trigger_name:
            query += " AND tgname = $2"
            params.append(trigger_name)

        rows = await self.conn.fetch(query, *params)
        return [row["function_name"] for row in rows]

    async def remove_trigger_function(self, function_name: str):
        """
        Removes a specific trigger function and depending triggers.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        query = f"DROP FUNCTION IF EXISTS {function_name} CASCADE;"
        await self.conn.execute(query)
        print(f"Trigger function '{function_name}' removed.")

    async def create_trigger(self, table_name: str, trigger_name: str, function_name: str, event: str, timing: str = "AFTER"):
        """
        Creates a PostgreSQL trigger for the specified table.
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

    async def get_triggers(self, table_name: str):
        """
        Retrieves all triggers associated with a specific table.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        query = """
        SELECT trigger_name
        FROM information_schema.triggers
        WHERE event_object_table = $1
        GROUP BY trigger_name
        ORDER BY trigger_name;
        """
        rows = await self.conn.fetch(query, table_name)
        return [row["trigger_name"] for row in rows]

    async def remove_trigger(self, table_name: str, trigger_name: str):
        """
        Removes a trigger from the specified table.
        """
        if self.conn is None:
            raise RuntimeError("Notifier not connected. Call `connect()` first.")

        query = f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};"
        await self.conn.execute(query)
        print(f"Trigger '{trigger_name}' removed from table '{table_name}'.")

    async def remove_function(self, function_name: str):
        """
        Removes a PostgreSQL function.
        """
        query = f"DROP FUNCTION IF EXISTS {function_name} CASCADE;"
        await self.conn.execute(query)
        print(f"Function '{function_name}' removed.")
