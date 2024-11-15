"""Implements Pipeline class."""
import abc
from typing import Any, Generic, Iterable, TypeVar

from .work_exception import WorkException

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
        

    def put(self, task:T|Exception):
        """Put *task* on the pipeline."""
        self._input_stage.put(task)
        

    def get(self, timeout:float|None=None)->Q|None:
        """Return result from the pipeline."""
        res = None
        for stage in self._output_stages:
            result = stage.get(timeout)
            if isinstance(result, WorkException):
                self.put(result)
                result.re_raise()
            if isinstance(result, KeyboardInterrupt):
                print("KeyboardInterrupt")
                return
            if isinstance(result, StopIteration):
                continue
            if isinstance(result, Exception):
                raise result
            res = result
        return res

    def results(self):
        """Return a generator to iterate over results from the pipeline."""
        while result := self.get():
            yield result

    def run(self,inputs: Iterable[T])->Iterable[Q]:
        for input in inputs:
            self._input_stage.put(input)
        self._input_stage.put(StopIteration())
        return self.results()