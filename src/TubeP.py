"""Implements TubeP class."""

import multiprocessing
from typing import Generic, TypeVar

from .Tube import Tube
T = TypeVar('T')


class TubeP(Tube[T]):
    """A unidirectional communication channel 
    using :class:`multiprocessing.Connection` for underlying implementation."""

    def __init__(self):
        (self._conn1, 
         self._conn2) = multiprocessing.Pipe(duplex=False)

    def put(self, data:T):
        """Put an item on the tube."""
        self._conn2.send(data)

    def get(self, timeout:float|None=None)->T:
        """Return the next available item from the tube.

        Blocks if tube is empty, until a producer for the tube puts an item on it."""
        if self._conn1.poll(timeout):
            return self._conn1.recv()
        else:
            raise multiprocessing.TimeoutError

