"""Implements Pipeline class."""
import abc
import multiprocessing
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
        self._output_stages = input_stage.get_leaves()
        self._input_stage.build()
        self.task_index=0
        self.lock=multiprocessing.Lock()
        

    def put(self, task:T|Exception):
        """Put *task* on the pipeline."""
        with self.lock:
            self._input_stage.put((self.task_index,task))
            self.task_index+=1
        

    def get(self, timeout:float|None=None)->tuple[int,Q]|None:
        """Return result from the pipeline."""
        
        for stage in self._output_stages:
            try:
                return stage.get(timeout)
                
            except StopIteration:
                pass
            
        return None

    def results(self):
        """Return a generator to iterate over results from the pipeline."""
        while result := self.get():
            task_index,res=result
            yield res
    
    def results_ordered(self):
        """Return a generator to iterate over results from the pipeline."""
        if len(self._output_stages) != 1:
            raise ValueError('Pipeline must have only one output stage.')
        current=0
        cache={}
        while result := self.get():
            task_index,res=result
            cache[task_index]=res
            while current in cache:
                yield cache[current]
                current+=1
            

    def run(self,inputs: Iterable[T],ordered_results:bool=False)->Iterable[Q]:
        for input in inputs:
            self.put(input)
        self.put(StopIteration())
        if ordered_results:
            return self.results_ordered()
        return self.results()