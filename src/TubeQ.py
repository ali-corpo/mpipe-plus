"""Implements TubeQ class."""

import multiprocessing
from typing import TypeVar

from .Tube import Tube

T = TypeVar('T')
class TubeQ(Tube[T]):
    """A unidirectional communication channel 
    using :class:`multiprocessing.Queue` for underlying implementation."""

    def __init__(self, maxsize=0):
        self._queue = multiprocessing.Queue(maxsize)

    def put(self, data:T):
        """Put an item on the tube."""
        self._queue.put(data)

    def get(self, timeout:float|None=None)->T:
        """Return the next available item from the tube.

        Blocks if tube is empty, until a producer for the tube puts an item on it."""
        
        try:
            return  self._queue.get(True, timeout)
        except multiprocessing.Queue.Empty:
            raise TimeoutError
        
        

    def close(self):
        self._queue.close()