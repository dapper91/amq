

class AMQError(Exception):
    """
    Base package exception.
    """


class NackMessage(AMQError):
    """
    Exception sending nack message to the amqp broker.

    :param requeue: if `True` - the task is requeued
    """

    def __init__(self, requeue=False):
        self.requeue = requeue
