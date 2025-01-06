import asyncio
from pubsub import Pub, Sub
from py_pg_notify import PGConfig, Notification
from dotenv import load_dotenv

load_dotenv() # Sets the environment variables for the PGConfig

config = PGConfig()

async def pubsub():
    subscriber = Sub(config)
    subs = await subscriber.subscribe(channel='Channel_1')
    publisher = Pub(config)
    for i in range (1, 10):
        message = f"Message #{i} - py-pg-notify PubSub example"
        await publisher.publish(channel='Channel_1', msg=message)
    await subs.close()

if __name__ == '__main__':
    asyncio.run(pubsub())


