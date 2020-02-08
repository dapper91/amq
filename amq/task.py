import abc
import functools as ft
from typing import Callable, Dict, Optional, Union


class Task(abc.ABC):
    """
    Class-based worker task.
    """

    def __init__(self, context):
        self.context = context

    async def __call__(self, *args, **kwargs):
        return await self.run(*args, **kwargs)

    @abc.abstractmethod
    async def run(self, *args, **kwargs):
        """
        Runs the task.
        """


class MethodRouter(Dict[str, Union[Callable, Task]]):
    """
    Method router.
    """

    def add(self, method_name: Optional[str] = None):
        """
        Adds a method to the router.

        :param method_name: method name
        """

        def wrapper(method: Union[Callable, Task]):

            @ft.wraps(method)
            def func():
                key = method_name or method.__name__
                self[key] = method

                return method

            return func

        return wrapper


methods = MethodRouter()
