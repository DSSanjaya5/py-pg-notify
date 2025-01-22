# 🛒 E-Commerce Inventory Management with py-pg-notify

This repository demonstrates the implementation of real-time inventory synchronization using `py-pg-notify`:

- 📜 **`inventory_notifier.py`**: Implements the logic for publishing stock update notifications to a PostgreSQL channel.
- 📥 **`inventory_listener.py`**: A listener that subscribes to stock update notifications and processes them accordingly (e.g., updating stock levels or alerting warehouse staff).

## ⚙️ Installation

To get started with `py-pg-notify`, install it along with the required dependencies:

```bash
pip install py-pg-notify python-dotenv
```

## 🔍 How it Works

### 🛒 E-Commerce Inventory Management
In this scenario, when a product's stock is updated (e.g., after a purchase or stock replenishment), a notification is sent to a specific channel. This allows multiple systems (e.g., website frontends, warehouse management systems) to react to the updated stock levels.

- **Publisher (Notifier)**: Sends notifications when the stock of a product is updated.
- **Subscriber (Listener)**: Listens for notifications, updates stock levels for customers, and triggers alerts when stock falls below a defined threshold.

### 📡 Pub/Sub Architecture with PostgreSQL
`py-pg-notify` leverages PostgreSQL’s LISTEN/NOTIFY mechanism to implement a Pub/Sub system for synchronizing the inventory.

- 📤 **Publisher**: Sends stock update notifications to a PostgreSQL channel.
- 📥 **Subscriber**: Listens for stock updates and processes them in real time (e.g., updating the frontend or notifying warehouse staff).

## 🛠️ Core Classes - `inventory_notifier.py` and `inventory_listener.py`

### ✉️ Inventory Notifier (Publisher)
The `inventory_notifier.py` script is responsible for publishing stock update notifications.

**Key Function**:
- `publish_stock_update(channel: str, product_id: int, new_stock: int)`: Publishes a stock update notification to the specified channel.

### 👂 Inventory Listener (Subscriber)
The `inventory_listener.py` script subscribes to the `inventory_update_channel` and handles incoming notifications.

**Key Functions**:
- `notification_handler(msg: Notification)`: Processes received notifications and updates stock levels.
- `start_listener()`: Starts listening for stock update notifications.

## 🚀 Example Usage

### 🔄 Workflow:
1. 🔧 **Setup**: Load PostgreSQL connection parameters from a `.env` file.
2. 👂 **Subscribe**: Start a listener on the `inventory_update_channel` for stock update notifications.
3. 📤 **Publish**: The notifier sends stock update notifications to the `inventory_update_channel` whenever stock levels change.
4. 🛠️ **Handle Messages**: The listener processes stock updates and checks if the stock falls below a defined threshold.
5. 📊 **Update Frontend/Notify Staff**: Update the frontend with the new stock level or alert warehouse staff if stock is low.

## 🏃‍♂️ How to Run

1. 🗄️ **Run the Database Setup:**
Before running the Notifier (Publisher), you need to run the `db_setup.py` script to set up the required tables and triggers in your PostgreSQL database.
```bash
python db_setup.py
```

2. 📁 **Set Up Environment:**
Create a `.env` file with your PostgreSQL configuration:
```dotenv
PG_USER=your_user
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=your_database
```

3. ▶️ **Run the Notifier (Publisher):** 
The notifier will publish stock updates to the `inventory_update_channel`:
```bash
python inventory_notifier.py
```

4. ▶️ **Run the Listener (Subscriber):** The listener will subscribe to the `inventory_update_channel` and process incoming stock updates:
```bash
python inventory_listener.py
```

## 🌍 Real-World Applications

The `py-pg-notify` library can be applied in various **E-Commerce Inventory Management** scenarios:

### 🌐 Multi-Warehouse Synchronization:
- When stock levels are updated in one warehouse, the notification is sent to all other systems (e.g., other warehouses or frontends) to keep inventory synchronized in real time.

### 📱 Frontend Stock Updates:
- As the stock of a product is updated, the website frontend automatically reflects the latest stock availability for customers.

### ⚠️ Low Stock Alerts:
- If the stock of a product falls below a threshold (e.g., 10 units), a notification can be sent to warehouse staff or suppliers to restock.

### 🛠️ Automated Replenishment:
- Set up automated alerts for low stock levels to trigger replenishment workflows.

## 🚀 Start Building Real-Time Inventory Systems Today!

With `py-pg-notify`, you can build an efficient, real-time inventory synchronization system for your e-commerce platform.

Happy coding! 🎉
