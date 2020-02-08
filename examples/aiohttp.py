import asyncio
import amq
from amq.integrations import aiohttp as amq_aiohttp
from aiohttp import web


@amq.methods.add
def task(timeout):
    print("hello world")


@amq.methods.add
async def task(timeout):
    await asyncio.sleep(timeout)


class Task(amq.Task):
    async def run(self, timeout):
        await asyncio.sleep(timeout)


async def handler(request: web.Request):
    await request.config_dict['amq'].submit(task, params=dict(timeo=5))
    return web.Response()


app = web.Application()
app.router.add_get('/', handler)
amq_aiohttp.setup(app)
