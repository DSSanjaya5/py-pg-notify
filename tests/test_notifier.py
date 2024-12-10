import pytest
from unittest.mock import AsyncMock, patch
from py_pg_notify.notifier import Notifier


@pytest.mark.asyncio
class TestNotifier:
    @pytest.fixture
    def mock_dsn(self):
        return "postgresql://user:password@localhost:5432/testdb"

    @pytest.fixture
    def mock_handler(self):
        async def handler(connection, pid, channel, payload):
            pass

        return handler

    async def test_notifier_initialization_with_dsn(self, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        assert notifier.dsn == mock_dsn
        assert notifier.conn is None

    async def test_notifier_initialization_without_dsn(self):
        notifier = Notifier(
            user="user",
            password="password",
            host="localhost",
            port=5432,
            dbname="testdb",
        )
        expected_dsn = "postgresql://user:password@localhost:5432/testdb"
        assert notifier.dsn == expected_dsn
        assert notifier.conn is None

    async def test_notifier_initialization_missing_params(self):
        with pytest.raises(ValueError):
            Notifier(user="user", password="password")  # Missing dbname

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()
        mock_connect.assert_called_once_with(mock_dsn)
        assert notifier.conn == mock_connect.return_value

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_already_connected(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        notifier.conn = AsyncMock()
        await notifier.connect()
        mock_connect.assert_not_called()  # No new connection should be created

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_create_trigger_function_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        await notifier.create_trigger_function("test_function", "test_channel")
        expected_query = """
        CREATE OR REPLACE FUNCTION test_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify(
                'test_channel',
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

        mock_connect.return_value.execute.assert_called_once_with(expected_query)

    async def test_create_trigger_function_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.create_trigger_function("test_function", "test_channel")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_get_trigger_functions_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        mock_connect.return_value.fetch.return_value = [
            {"function_name": "test_function"}
        ]
        functions = await notifier.get_trigger_functions("test_table")
        assert functions == ["test_function"]

    async def test_get_trigger_functions_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.get_trigger_functions("test_table")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_trigger_function_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        await notifier.remove_trigger_function("test_function")
        mock_connect.return_value.execute.assert_called_once_with(
            "DROP FUNCTION IF EXISTS test_function CASCADE;"
        )

    async def test_remove_trigger_function_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.remove_trigger_function("test_function")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_create_trigger_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        await notifier.create_trigger(
            "test_table", "test_trigger", "test_function", "INSERT"
        )
        expected_query = """
        CREATE TRIGGER test_trigger
        AFTER INSERT ON test_table
        FOR EACH ROW
        EXECUTE FUNCTION test_function();
        """

        mock_connect.return_value.execute.assert_called_once_with(expected_query)

    async def test_create_trigger_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.create_trigger(
                "test_table", "test_trigger", "test_function", "INSERT"
            )

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_get_triggers_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        mock_connect.return_value.fetch.return_value = [
            {"trigger_name": "test_trigger"}
        ]
        triggers = await notifier.get_triggers("test_table")
        assert triggers == ["test_trigger"]

    async def test_get_triggers_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.get_triggers("test_table")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_trigger_successful(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)
        await notifier.connect()

        await notifier.remove_trigger("test_table", "test_trigger")
        mock_connect.return_value.execute.assert_called_once_with(
            "DROP TRIGGER IF EXISTS test_trigger ON test_table;"
        )

    async def test_remove_trigger_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await notifier.remove_trigger("test_table", "test_trigger")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_context_manager(self, mock_connect, mock_dsn):
        notifier = Notifier(dsn=mock_dsn)

        async with notifier as n:
            assert n.conn == mock_connect.return_value

        mock_connect.return_value.close.assert_called_once()

    async def test_close_without_connection(self):
        notifier = Notifier(dsn="mock_dsn")
        await notifier.close()  # Should not raise an error if no connection exists
