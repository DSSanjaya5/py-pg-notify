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

### Listener Example
```python
import asyncio
from py_pg_notify import Listener

async def notification_handler(connection, pid, channel, payload):
    # Perform any processing on the received notification
    print(f"Notification received: Channel={channel}, Payload={payload}")

async def main():
    listener = Listener(user="postgres", password="1234", dbname="pg_test")
    async with listener:
        await listener.add_listener("ch_01", notification_handler)
        await asyncio.sleep(3600)  # Simulate long-running process

if __name__ == "__main__":
    asyncio.run(main())
```
### Notifies (Work In-Progress)

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
4. Submit a pull request.

---

## 📢 Feedback and Support
For bugs or feature requests, create an issue in the GitHub repository. We'd love to hear your feedback!