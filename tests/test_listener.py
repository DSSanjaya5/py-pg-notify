import os
import pytest
from unittest.mock import AsyncMock, patch
from py_pg_notify.listener import Listener, Notification
from py_pg_notify.pgmanager import PGConfig


@pytest.mark.asyncio
class TestListener:
    @pytest.fixture
    def mock_dsn(self):
        return "postgresql://user:password@localhost:5432/testdb"

    @pytest.fixture
    def mock_handler(self):
        async def handler(notification):
            pass
        return handler

    @pytest.fixture
    def mock_config(self, mock_dsn):
        return PGConfig(dsn=mock_dsn)

    # Existing test cases
    async def test_listener_initialization_with_dsn(self, mock_config):
        listener = Listener(mock_config)
        assert listener.dsn == mock_config.dsn
        assert listener.conn is None
        assert listener.listeners == {}

    async def test_listener_initialization_without_dsn(self):
        mock_config = PGConfig(user="user", password="password", host="localhost", port=5432, dbname="testdb")
        listener = Listener(mock_config)
        expected_dsn = "postgresql://user:password@localhost:5432/testdb"
        assert listener.dsn == expected_dsn
        assert listener.conn is None

    async def test_listener_initialization_missing_params(self):
        with pytest.raises(ValueError):
            PGConfig(user="user", password="password")  # Missing dbname

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_successful(self, mock_connect, mock_config):
        listener = Listener(mock_config)
        await listener.connect()
        mock_connect.assert_called_once_with(mock_config.dsn)
        assert listener.conn == mock_connect.return_value

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_connect_already_connected(self, mock_connect, mock_config):
        listener = Listener(mock_config)
        listener.conn = AsyncMock()
        await listener.connect()
        mock_connect.assert_not_called()  # No new connection should be created

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_add_listener_successful(self, mock_connect, mock_config, mock_handler):
        listener = Listener(mock_config)
        await listener.connect()

        await listener.add_listener("test_channel", mock_handler)
        assert "test_channel" in listener.listeners
        mock_connect.return_value.add_listener.assert_called_once_with(
            "test_channel", listener.listeners["test_channel"]
        )

    async def test_add_listener_without_connection(self, mock_handler, mock_config):
        listener = Listener(mock_config)
        with pytest.raises(RuntimeError):
            await listener.add_listener("test_channel", mock_handler)

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_listener_successful(self, mock_connect, mock_config, mock_handler):
        listener = Listener(mock_config)
        await listener.connect()

        await listener.add_listener("test_channel", mock_handler)
        wrapped_handler = listener.listeners["test_channel"]  # Get the wrapped handler
        await listener.remove_listener("test_channel")
        assert "test_channel" not in listener.listeners
        mock_connect.return_value.remove_listener.assert_called_once_with(
            "test_channel", wrapped_handler  # Use the wrapped handler here
        )

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_remove_nonexistent_listener(self, mock_connect, mock_config):
        listener = Listener(mock_config)
        await listener.connect()
        mock_connect.assert_called_once()
        with pytest.raises(
            KeyError, match="No listener found for channel 'nonexistent_channel'."
        ):
            await listener.remove_listener("nonexistent_channel")

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_close_successful(self, mock_connect, mock_config):
        listener = Listener(mock_config)
        await listener.connect()

        await listener.add_listener("test_channel", AsyncMock())
        await listener.close()

        assert listener.conn is None
        assert listener.listeners == {}
        mock_connect.return_value.close.assert_called_once()

    async def test_close_without_connection(self, mock_config):
        listener = Listener(mock_config)
        await listener.close()  # Should not raise an error if no connection exists

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_context_manager(self, mock_connect, mock_config):
        listener = Listener(mock_config)

        async with listener as l:
            assert l.conn == mock_connect.return_value
        # Check if the connection was closed at the end of the context
        mock_connect.return_value.close.assert_called_once()

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_notification_callback_execution(self, mock_connect, mock_config):
        listener = Listener(mock_config)
        callback_mock = AsyncMock()
        await listener.connect()

        await listener.add_listener("test_channel", callback_mock)

        # Simulate a notification
        notification = Notification(None, 12345, "test_channel", '{"key": "value"}')
        await listener.listeners["test_channel"](None, 12345, "test_channel", '{"key": "value"}')

        callback_mock.assert_awaited_once()
        notification = callback_mock.await_args.args[0]  # Get the first argument passed to the callback
        assert notification.channel == "test_channel"
        assert notification.payload == '{"key": "value"}'
        assert notification.pid == 12345

    @pytest.fixture
    def config_dict(self):
        return {
            "user": "myuser",
            "password": "mypassword",
            "host": "localhost",
            "port": 5432,
            "dbname": "mydb"
        }

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_listener_connection_with_dict(self, mock_connect, config_dict):
        # Initialize Listener with a dictionary
        config = PGConfig(**config_dict)
        listener = Listener(config)

        # Simulate connection
        await listener.connect()

        # Verify connection call
        mock_connect.assert_called_once_with(config.dsn)
        assert listener.conn == mock_connect.return_value

    @pytest.fixture
    def set_env_vars(self):
        # Set environment variables for the database configuration
        os.environ["PG_USER"] = "myuser"
        os.environ["PG_PASSWORD"] = "mypassword"
        os.environ["PG_HOST"] = "localhost"
        os.environ["PG_PORT"] = "5432"
        os.environ["PG_DBNAME"] = "mydb"
        yield
        # Clean up environment variables after test
        del os.environ["PG_USER"]
        del os.environ["PG_PASSWORD"]
        del os.environ["PG_HOST"]
        del os.environ["PG_PORT"]
        del os.environ["PG_DBNAME"]

    @patch("asyncpg.connect", new_callable=AsyncMock)
    async def test_listener_connection_with_env_vars(self, mock_connect, set_env_vars):
        # Initialize Listener using environment variables
        config = PGConfig(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT")),
            dbname=os.getenv("PG_DBNAME")
        )
        listener = Listener(config)

        # Simulate connection
        await listener.connect()

        # Verify connection call
        mock_connect.assert_called_once_with(config.dsn)
        assert listener.conn == mock_connect.return_value
