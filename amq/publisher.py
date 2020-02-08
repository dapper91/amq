import json
from typing import Callable, Type

import aio_pika


class Publisher:
    """
    Message publisher.

    :param connection: broker connection
    :param queue_name: message queue name
    :param delivery_mode: message delivery mode (persistent by default)
    """

    def __init__(
        self,
        connection: aio_pika.Connection,
        queue_name: str,
        delivery_mode: aio_pika.DeliveryMode = aio_pika.DeliveryMode.PERSISTENT,
        dumps: Callable = json.dumps,
        json_encoder: Type[json.JSONEncoder] = json.JSONEncoder,
    ):
        self._queue_name = queue_name
        self._connection = connection
        self._delivery_mode = delivery_mode
        self._dumps = dumps
        self._json_encoder = json_encoder

    async def submit(self, method: str, params, **message_kwargs):
        """
        Publish a task to the queue for execution.

        :param method: method name
        :param params: method parameters
        :param message_kwargs: additional message arguments
        """

        message = aio_pika.Message(
            body=self._dumps({
                'jsonrpc': '2.0',
                'method': method,
                'params': params,
            }, cls=self._json_encoder).encode(),
            content_encoding='utf8',
            content_type='application/json',
            delivery_mode=self._delivery_mode,
            **message_kwargs
        )

        async with self._connection.channel() as channel:
            await channel.default_exchange.publish(message, self._queue_name)
