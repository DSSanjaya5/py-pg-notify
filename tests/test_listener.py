import pytest
from unittest.mock import AsyncMock, patch
from py_pg_notify.listener import Listener


@pytest.mark.asyncio
class TestListener:
    @pytest.fixture
    def mock_dsn(self):
        return "postgresql://user:password@localhost:5432/testdb"

    @pytest.fixture
    def mock_handler(self):
        async def handler(connection, pid, channel, payload):
            pass

        return handler

    async def test_listener_initialization_with_dsn(self, mock_dsn):
        listener = Listener(dsn=mock_dsn)
        assert listener.dsn == mock_dsn
        assert listener.conn is None
        assert listener.listeners == {}

    async def test_listener_initialization_without_dsn(self):
        listener = Listener(
            user="user",
            password="password",
            host="localhost",
            port=5432,
            dbname="testdb",
        )
        expected_dsn = "postgresql://user:password@localhost:5432/testdb"
        assert listener.dsn == expected_dsn
        assert listener.conn is None

    async def test_listener_initialization_missing_params(self):
        with pytest.raises(ValueError):
            Listener(user="user", password="password")  # Missing dbname

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_successful(self, mock_connect, mock_dsn):
        listener = Listener(dsn=mock_dsn)
        await listener.connect()
        mock_connect.assert_called_once_with(mock_dsn)
        assert listener.conn == mock_connect.return_value

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_already_connected(self, mock_connect, mock_dsn):
        listener = Listener(dsn=mock_dsn)
        listener.conn = AsyncMock()
        await listener.connect()
        mock_connect.assert_not_called()  # No new connection should be created

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_add_listener_successful(self, mock_connect, mock_dsn, mock_handler):
        listener = Listener(dsn=mock_dsn)
        await listener.connect()

        await listener.add_listener("test_channel", mock_handler)
        assert "test_channel" in listener.listeners
        mock_connect.return_value.add_listener.assert_called_once_with(
            "test_channel", listener.listeners["test_channel"]
        )

    async def test_add_listener_without_connection(self, mock_handler):
        listener = Listener(dsn="mock_dsn")
        with pytest.raises(RuntimeError):
            await listener.add_listener("test_channel", mock_handler)

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_listener_successful(
        self, mock_connect, mock_dsn, mock_handler
    ):
        listener = Listener(dsn=mock_dsn)
        await listener.connect()

        await listener.add_listener("test_channel", mock_handler)
        wrapped_handler = listener.listeners["test_channel"]  # Get the wrapped handler
        await listener.remove_listener("test_channel")
        assert "test_channel" not in listener.listeners
        mock_connect.return_value.remove_listener.assert_called_once_with(
            "test_channel", wrapped_handler  # Use the wrapped handler here
        )

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_nonexistent_listener(self, mock_connect):
        listener = Listener(dsn="postgresql://user:password@localhost:5432/testdb")
        await listener.connect()
        mock_connect.assert_called_once()
        with pytest.raises(
            KeyError, match="No listener found for channel 'nonexistent_channel'."
        ):
            await listener.remove_listener("nonexistent_channel")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_close_successful(self, mock_connect, mock_dsn):
        listener = Listener(dsn=mock_dsn)
        await listener.connect()

        await listener.add_listener("test_channel", AsyncMock())
        await listener.close()

        assert listener.conn is None
        assert listener.listeners == {}
        mock_connect.return_value.close.assert_called_once()

    async def test_close_without_connection(self):
        listener = Listener(dsn="mock_dsn")
        await listener.close()  # Should not raise an error if no connection exists

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_context_manager(self, mock_connect, mock_dsn):
        listener = Listener(dsn=mock_dsn)

        async with listener as l:
            assert l.conn == mock_connect.return_value
        # Check if the connection was closed at the end of the context
        mock_connect.return_value.close.assert_called_once()

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_notification_callback_execution(self, mock_connect, mock_dsn):
        listener = Listener(dsn=mock_dsn)
        callback_mock = AsyncMock()
        await listener.connect()

        await listener.add_listener("test_channel", callback_mock)

        # Simulate a notification
        await listener.listeners["test_channel"](
            None, 12345, "test_channel", '{"key": "value"}'
        )
        callback_mock.assert_awaited_once_with(
            None, 12345, "test_channel", '{"key": "value"}'
        )
