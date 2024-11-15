"""Implements TubeP class."""

import abc
from typing import Generic, TypeVar
T = TypeVar('T')

class Tube(abc.ABC,Generic[T]):
    @abc.abstractmethod
    def put(self, data:T):
        """Put an item on the tube."""
        pass

    @abc.abstractmethod
    def get(self, timeout:float|None=None)->T:
        """Return the next available item from the tube."""
        pass

    @abc.abstractmethod
    def close(self):
        """Close the tube."""
        pass