# 📬 PubSub Example with py-pg-notify

This repository demonstrates the implementation of a simple PubSub system using `py-pg-notify`:

- 📜 **`pubsub.py`**: Implements the `Pub` and `Sub` classes for publishing and subscribing.
- 💡 **`app.py`**: A complete example showcasing a simple Pub/Sub system in action.


## ⚙️ Installation

To use `py-pg-notify`, first install it along with the required dependencies:

```bash
pip install py-pg-notify python-dotenv colorama
```

## 🔍 How it works

### 📡 Pub/Sub Architecture
In a Pub/Sub architecture:

* 📤 Publisher sends messages to a specific channel.
* 📥 Subscriber listens to the channel and reacts to the messages received.

`py-pg-notify` leverages PostgreSQL's notification mechanism to implement this architecture seamlessly.

## 🛠️ Core Classes - `pubsub.py`

### ✉️ Pub (Publisher)
The `Pub` class is responsible for sending notifications to a `channel`.

Key Method:
* `publish(channel: str, msg: str)`: Publishes a message to the specified channel.

### 👂Sub (Subscriber)
The `Sub` class subscribes to a `channel` and handles incoming notifications.

Key Method:
* 🔔 `subscribe(channel: str, msg_handler: Callable = None)`: Subscribes to a channel.
* ⚙️ `default_msg_handler(msg: Notification)`: Default handler for processing messages. This won't be used if a custom callback is provided in the subscribe function
* ❌ `unsubscribe(channel: str)`: Unsubscribes from a channel.

## 🚀 Example Usage - `app.py `

The `app.py` file demonstrates how to use the Pub and Sub classes.

### 🔄 Workflow:
1. 🔧 **Setup:** Load PostgreSQL connection parameters from a `.env` file.
2. 👂 **Subscribe:** Start a subscriber on the `Channel_1`.
3. 📤 **Publish:** Send 10 messages to the `Channel_1`.
4. 🛠️ **Handle Messages:** Process messages with a custom or default handler.
5. ❌ **Unsubscribe:** Cleanly close the subscription.

## 🏃‍♂️ How to Run

1. 📁 **Set Up Environment:** Create a `.env` file with your PostgreSQL configuration:
```dotenv
PG_USER=your_user
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=your_database
```
2. ▶️ **Run the Example:** Execute the `app.py` file:
```bash
python app.py
```

## 🌍 Real-World Applications
The `py-pg-notify` library can be applied in various scenarios:

1. 🌐 **IoT (Internet of Things):**
   - Use Pub/Sub to manage communication between IoT devices and backend systems.
   - For example, sensors can publish temperature data, and a monitoring service can subscribe to analyze and alert if the temperature crosses thresholds.

2. 📊 **Real-Time Analytics:**
   - Collect and process real-time data streams for analytics dashboards.
   - For instance, a log monitoring system can publish log events, and different services (like anomaly detection and reporting) can subscribe to analyze and visualize the data.

3. 📋 **Task Queues:**
    * Use notifications to signal workers for new tasks.

4. 🔄 **Workflow Automation:**
   - Trigger workflow steps or pipelines based on events.
   - For example, a CI/CD pipeline can use Pub/Sub to notify downstream services when a code build is completed or a deployment starts.

Start building real-time, event-driven systems today with py-pg-notify! 🚀