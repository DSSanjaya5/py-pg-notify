# py-pg-notify

**py-pg-notify** is a Python library that simplifies listening to and sending notifications using PostgreSQL's `LISTEN/NOTIFY` functionality. This package leverages `asyncpg` for asynchronous communication with the database, making it ideal for real-time applications.

---

## 📖 Features

- 🔊 **PostgreSQL Notifications**: Easy-to-use interfaces for `LISTEN/NOTIFY`.
- 🔄 **Asynchronous**: Fully asynchronous with support for multiple listeners and channels.
- 📦 **Lightweight**: Built on top of `asyncpg`, offering high performance.
- ⚙️ **Custom Handlers**: Define your notification handling logic dynamically.

---

## 🚀 Installation

Install the package via pip:

```bash
pip install py-pg-notify
```

---

## Usage

### Configuration Example
You can use `PGConfig` to manage PostgreSQL connection parameters via a dictionary, environment variables, or direct parameters.
```python
from py_pg_notify import PGConfig

# Example using a dictionary
config_dict = {
    "user": "<username>",
    "password": "<password>",
    "host": "<host>",
    "port": 5432,
    "dbname": "<dbname>"
}
config = PGConfig(config_dict=config_dict)

# Example using environment variables
# Ensure PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, and PG_DBNAME are set in your environment
config = PGConfig()

# Example using direct parameters
config = PGConfig(user="<username>", password="<password>", host="<host>", port=5432, dbname="<dbname>")
```

### Notifier Example
```python
import asyncio
from py_pg_notify import Notifier, PGConfig

async def create_trigger_example():
    # Define the configuration using environment variables
    config = PGConfig()

    async with Notifier(config) as notifier:

        await notifier.create_trigger_function("notify_function", "ch_01")
        
        await notifier.create_trigger(
            table_name="my_table",
            trigger_name="my_trigger",
            function_name="notify_function",
            event="INSERT",
        )

        trigger_functions = await notifier.get_trigger_functions("my_table")
        print("Existing Trigger Functions:", trigger_functions)

        triggers = await notifier.get_triggers("my_table")
        print("Existing Triggers:", triggers)

        await notifier.remove_trigger("my_table", "my_trigger")

        await notifier.remove_trigger_function("notify_function")

if __name__ == "__main__":
    asyncio.run(create_trigger_example())
```

### Listener Example
```python
import asyncio
from py_pg_notify import Listener, PGConfig

async def notification_handler(connection, pid, channel, payload):
    # Perform any processing on the received notification
    print(f"Notification received: Channel={channel}, Payload={payload}")

async def main():
    # Define the configuration using environment variables
    config = PGConfig()

    async with Listener(config) as listener:
        await listener.add_listener("ch_01", notification_handler)
        await asyncio.sleep(3600)  # Simulate long-running process

if __name__ == "__main__":
    asyncio.run(main())
```

### Complete Example (Work In-Progress)
Refer to the examples/ folder for complete usage scenarios.

---

## 🧪 Testing
Run the test suite using pytest:

```bash
pip install -r requirements_test.txt
pytest --asyncio-mode=auto
```

---

## 📄 License
This project is licensed under the MIT License. See the LICENSE file for details.

---

## 🤝 Contributing
We welcome contributions! Please follow these steps:

1. Fork the repository.
2. Create a new branch.
3. Make your changes.
4. Add the test cases if needed.
5. Test all the test cases for verfication.
6. Submit a pull request.

---

## 📢 Feedback and Support
For bugs or feature requests, create an issue in the GitHub repository. We'd love to hear your feedback!