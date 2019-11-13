import asyncio
import json
import uuid
from datetime import datetime


class ResponseQueue(asyncio.Queue):

    def __init__(self, *args, **kwargs):
        self.done = False
        super().__init__(*args, **kwargs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.done:
            raise StopAsyncIteration
        return await self.get()


async def request(queue, duration, repeat, run_uuid):
    idx = 0
    while idx < repeat:
        await asyncio.sleep(duration)
        idx += 1
        print(f"{datetime.now()} {run_uuid}: putting iteration {idx} on the queue")
        await queue.put(f"{run_uuid} iteration {idx}")
    queue.done = True
    print(f"{datetime.now()} {run_uuid}: finished job")


def execute(message, config):
    run_uuid = uuid.uuid4()
    print(f"{datetime.now()} {run_uuid}: received job")
    loop = asyncio.get_event_loop()
    queue = ResponseQueue(loop=loop)
    payload = json.loads(message.raw_payload)
    loop.create_task(request(queue,
                             payload.pop("duration", 30),
                             payload.pop("repeat", 1),
                             run_uuid))
    return queue
