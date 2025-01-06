from datetime import datetime
from typing import Callable
from py_pg_notify import PGConfig, Notifier, Listener, Notification

class Pub:

    def __init__(self, config: PGConfig):
        self.config = config

    async def publish(self, channel: str, msg: str):
        try:
            async with Notifier(self.config) as publisher:
                await publisher.notify(channel, msg)
        except Exception as e:
            raise e
        

class Sub:

    def __init__(self, config: PGConfig):
        self.config = config

    async def default_msg_handler(self, msg: Notification):
        print(f"[{datetime.now()}] Channel: {msg.channel} | Message Received: {msg.payload}")

    async def subscribe(self, channel: str, msg_handler: Callable = None):
        try: 
            msg_handler = msg_handler if msg_handler else self.default_msg_handler
            subscriber = Listener(self.config)
            await subscriber.connect()
            await subscriber.add_listener(channel, msg_handler)
            return subscriber
        except Exception as e:
            raise e 

    async def unsubscribe(self, channel: str):
        try:
            async with Listener(self.config) as unsubscriber:
                await unsubscriber.remove_listener(channel)
        except Exception as e:
            raise e
