"""Implements Pipeline class."""
import abc
from typing import Any, Generic, Iterable, TypeVar

from . import Stage


T = TypeVar('T')
Q = TypeVar('Q')
Z = TypeVar('Z')


class TPipeline(Generic[T,Q]):
    """A pipeline of stages."""
    def __init__(self, init_stage: Stage[T,Q]):
        """Constructor takes the root upstream stage."""
        self._input_stage = init_stage


    def then(self, next_stage:Stage[Q,Z])->"TPipeline[T,Z]":
        """Convenience method to link to the given downstream stage *next_stage*."""
        for stage in self._input_stage.getLeaves():
            stage.link(next_stage)
        return TPipeline[T,Z](self._input_stage)#type: ignore


    def run(self,inputs: Iterable[T],timeout:float|None=None):
        output_stages=self._input_stage.getLeaves()
        for input in inputs:
            self._input_stage.put(input)
        self._input_stage.put(None)

        assert len(output_stages)==1    
        stage=output_stages[0]
        while res:=stage.get(timeout):
            yield res
        


        

    def _get(self, timeout:float|None=None)->Q|None:
        """Return result from the pipeline."""
        result = None
        for stage in self._output_stages:
            result = stage.get(timeout)
        return result

    def results(self):
        """Return a generator to iterate over results from the pipeline."""
        while result := self.get():
            yield result

