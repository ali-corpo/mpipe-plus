"""Implements Pipeline class."""
import abc
from typing import Any, Generic, Iterable, TypeVar

from . import Stage


T = TypeVar('T')
Q = TypeVar('Q')


class Pipeline(Generic[T,Q]):
    """A pipeline of stages."""
    def __init__(self, input_stage: Stage[T,Any]):
        """Constructor takes the root upstream stage."""
        self._input_stage = input_stage
        self._output_stages = input_stage.getLeaves()
        self._input_stage.build()
        

    def put(self, task:T):
        """Put *task* on the pipeline."""
        self._input_stage.put(task)
        

    def get(self, timeout:float|None=None)->Q|None:
        """Return result from the pipeline."""
        result = None
        for stage in self._output_stages:
            result = stage.get(timeout)
        return result

    def results(self):
        """Return a generator to iterate over results from the pipeline."""
        while result := self.get():
            yield result

    def run(self,inputs: Iterable[T])->Iterable[Q]:
        for input in inputs:
            self._input_stage.put(input)
        self._input_stage.put(None)
        return self.results()