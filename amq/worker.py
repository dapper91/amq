import asyncio
import inspect
import json
import functools as ft
import logging
from typing import Any, Optional, Set

import aio_pika
import async_timeout

from amq import exceptions, task

logger = logging.getLogger(__package__)


def shield(func):
    """
    Decorator, preserving function from cancellation.
    """

    @ft.wraps(func)
    async def wrapped(*args, **kwargs):
        return await asyncio.shield(func(*args, **kwargs))

    return wrapped


class Worker:
    """
    Background task worker.

    :param connection: broker connection
    :param queue_name: task queue name
    :param queue_arguments: additional queue arguments
    :param passive: if `True` - exchange and queues will not be created
    :param attempts: maximum task execution attempts
    :param dlx_name: dlx exchange name
    :param dlx_queue_name: dlx queue name, see. https://www.rabbitmq.com/dlx.html
    :param dlx_queue_arguments: dlx queue additional arguments
    :param prefetch_count: worker prefectch count
    :param task_context: application context to be passed to a task
    """

    def __init__(
        self,
        connection: aio_pika.Connection,
        queue_name: str,
        queue_arguments: Optional[dict] = None,
        passive: bool = True,
        attempts: int = 3,
        dlx_name: str = 'dlx',
        dlx_queue_name: Optional[str] = None,
        dlx_queue_arguments: Optional[dict] = None,
        prefetch_count: int = 10,
        task_context: Optional[Any] = None,
    ):
        self._connection = connection
        self._channel: Optional[aio_pika.Channel] = None
        self._active_tasks: Set[asyncio.Task] = set()

        self._queue_name = queue_name
        self._queue_arguments = queue_arguments or {}
        self._queue: Optional[aio_pika.Queue] = None
        self._consumer_tag: Optional[aio_pika.queue.ConsumerTag] = None

        self._dlx_name = dlx_name
        self._dlx_queue_name = dlx_queue_name
        self._dlx_queue_arguments = dlx_queue_arguments or {}

        self._prefetch_count = prefetch_count
        self._passive = passive
        self._attempts = attempts
        self._task_context = task_context

    async def __aenter__(self) -> 'Worker':
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.shutdown()

    @property
    def active_tasks(self) -> set:
        return self._active_tasks

    async def start(self):
        """
        Runs the worker.
        """

        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch_count)

        if self._dlx_queue_name:
            dlx = await self._channel.declare_exchange(self._dlx_name, durable=True, passive=self._passive)
            dlq = await self._channel.declare_queue(
                self._dlx_queue_name, durable=True, passive=self._passive, arguments=self._dlx_queue_arguments,
            )
            await dlq.bind(dlx, self._queue_name)

            self._queue_arguments['x-dead-letter-exchange'] = dlx.name

        self._queue = await self._channel.declare_queue(
            self._queue_name, durable=True, passive=self._passive, arguments=self._queue_arguments,
        )

        def _on_message_received_wrapper(message: aio_pika.IncomingMessage):
            task = asyncio.create_task(self._on_message_received(message))

            self._active_tasks.add(task)
            task.add_done_callback(lambda fut: self._active_tasks.remove(task))

        self._consumer_tag = await self._queue.consume(_on_message_received_wrapper)

    @shield
    async def shutdown(self, graceful_timeout: float = 10.0, timeout: float = 15.0):
        """
        Shutdowns the worker.

        :param graceful_timeout: active task completion wait time.
        :param timeout: shutdown timeout.
        """

        logger.info("worker shutting down...")

        with async_timeout.timeout(timeout):
            await self._queue.cancel(self._consumer_tag)

            if self._active_tasks:
                await asyncio.wait(self._active_tasks, timeout=graceful_timeout)
                for task in self._active_tasks:
                    if not task.cancelled():
                        logger.warning("cancelling worker task")
                        task.cancel()

            if self._active_tasks:
                await asyncio.wait(self._active_tasks)

            await self._channel.close()

    async def _on_message_received(self, message: aio_pika.IncomingMessage):
        logger.debug("received a message: %r", message.body)

        with message.process(requeue=False, ignore_processed=True):
            try:
                if message.content_type != 'application/json':
                    raise ValueError('message content is not of type application/json')

                request = json.loads(message.body.decode(message.content_encoding or 'utf8'))
                await self._call(request['method'], request['params'])

            except exceptions.NackMessage as e:
                message.headers = message.headers or {}
                message.priority = message.priority or 0

                redelivered_count = message.headers.get('x-redelivered-count', 0)
                if e.requeue is False or redelivered_count >= self._attempts:
                    message.reject(requeue=False)
                else:
                    message.headers['x-redelivered-count'] = redelivered_count + 1
                    message.priority += 1
                    message.reject(requeue=True)

            except Exception as e:
                logger.exception("message processing error: %s\nbody: %r", e, message.body)
                message.reject(requeue=False)

    async def _call(self, method_name: str, params: dict):
        if method_name not in task.methods:
            raise RuntimeError(f"method '{method_name}' not found")

        logger.debug("executing task '%s' (params=%r)", method_name, params)

        method = task.methods[method_name]

        if inspect.iscoroutinefunction(method):
            await method(context=self._task_context, **params)

        elif inspect.isfunction(method):
            method(context=self._task_context, **params)

        elif inspect.isclass(method) and issubclass(method, task.Task):
            await method(self._task_context).run(**params)

        else:
            raise RuntimeError(f"unknown task type: {task}")
