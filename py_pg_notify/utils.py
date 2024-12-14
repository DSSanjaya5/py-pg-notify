# Utility queries


def create_trigger_function_query(function_name, channel):
    return f"""
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


GET_TRIGGER_FUNCTIONS_QUERY = """
SELECT pg_proc.proname AS function_name
FROM pg_trigger
INNER JOIN pg_class ON pg_class.oid = tgrelid
INNER JOIN pg_proc ON pg_proc.oid = tgfoid
WHERE pg_class.relname = $1
  AND NOT tgisinternal
"""

GET_TRIGGERS_QUERY = """
SELECT trigger_name
FROM information_schema.triggers
WHERE event_object_table = $1
GROUP BY trigger_name
ORDER BY trigger_name;
"""


def drop_function_query(function_name):
    return f"DROP FUNCTION IF EXISTS {function_name} CASCADE;"


def create_trigger_query(table_name, trigger_name, function_name, event, timing):
    return f"""
    CREATE TRIGGER {trigger_name}
    {timing} {event} ON {table_name}
    FOR EACH ROW
    EXECUTE FUNCTION {function_name}();
    """


def drop_trigger_query(trigger_name, table_name):
    return f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};"
