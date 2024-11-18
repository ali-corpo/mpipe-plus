"""Implements Stage class."""

import multiprocessing
from typing import Generic, Iterable, Self, TypeVar

from mpipe_plus import Worker
from mpipe_plus.work_exception import WorkException

from .tube import Tube

T = TypeVar('T')
Q = TypeVar('Q')
Z = TypeVar('Z')


class Stage(Generic[T, Q]):
    """The Stage is an assembly of workers of identical functionality."""

    def __init__(
        self, 
        worker_class, 
        num_worker=1,
        multi_process=True,
        disable_result=False,
        # do_stop_task=False, 
        input_tube:Tube[tuple[tuple[int,T|Exception],int]]|None=None,
        name=None,
        show_time=False,
        **worker_args
        ):
        """Create a stage of workers of given *worker_class* implementation, 
        with *size* indicating the number of workers within the stage.
        *disable_result* overrides any result defined in worker implementation,
        and does not propagate it downstream (equivalent to the worker
        producing ``None`` result).

        *do_stop_task* indicates whether the incoming "stop" signal (``None`` value)
        will actually be passed to the worker as a task. When using this option,
        implement your worker so that, in addition to regular incoming tasks,
        it handles the ``None`` value as well. This will be
        the worker's final task before the process exits.
        
        Any worker initialization arguments are given in *worker_args*."""
        self._worker_class = worker_class
        self._worker_args = worker_args
        self._num_worker = num_worker
        self._disable_result = disable_result
        # self._do_stop_task = do_stop_task
        self._input_tube:Tube[tuple[tuple[int,T|Exception],int]] = self._worker_class.getTubeClass()() \
                           if not input_tube else input_tube
        self._output_tubes = list[Tube[tuple[tuple[int,Q|Exception],int]]]()
        self.show_time=show_time
        self._next_stages = list[Stage]()
        self.name=name or self._worker_class.__name__
        self.multi_process=multi_process
        
    def put(self, task:tuple[int,T|Exception]):
        """Put *task* on the stage's input tube."""
        
        self._input_tube.put((task,0))

    def get(self, timeout:float|None=None)->tuple[int,Q]:
        """Retrieve results from all the output tubes."""
        for out in list(self.available_output_tubes):
            task_index,result = out.get(timeout)[0]
            if isinstance(result, StopIteration):
                self.available_output_tubes.remove(out)
                continue
            if isinstance(result, WorkException):
                self.workers_pool.terminate()
                result.re_raise()
            
            if isinstance(result, Exception):
                self.workers_pool.terminate()
                raise result
            return task_index,result

        self.workers_pool.join()
        raise StopIteration()
        

    def results(self)->Iterable[tuple[int,Q]]:
        """Return a generator to iterate over results from the stage."""
        try:
            while result := self.get():
                yield result
        except StopIteration:
            pass

    def link(self, next_stage:"Stage[Q,Z]")  -> Self:
        """Link to the given downstream stage *next_stage*
        by adding its input tube to the list of this stage's output tubes.
        Return this stage."""
        if next_stage is self: raise ValueError('cannot link stage to itself')
        self._output_tubes.append(next_stage._input_tube)
        self._next_stages.append(next_stage)
        return self

    def get_leaves(self)->list["Stage"]:
        """Return the downstream leaf stages of this stage."""
        result = list[Stage]()
        if not self._next_stages:
            result.append(self)
        else:
            for stage in self._next_stages:
                leaves = stage.get_leaves()
                result += leaves
        return result

    def build(self):
        """Create and start up the internal workers."""

        # If there's no output tube, it means that this stage
        # is at the end of a fork (hasn't been linked to any stage downstream).
        # Therefore, create one output tube.
        if not self._output_tubes:
            self._output_tubes.append(self._worker_class.getTubeClass()())

        
        # Create the workers.
        print("num_worker",self._num_worker)
        if self.multi_process:
            self.workers_pool = multiprocessing.Pool(self._num_worker)
        else:
            from multiprocessing.pool import ThreadPool
            self.workers_pool = ThreadPool(self._num_worker)
        for i in range(self._num_worker):
            self.workers_pool.apply_async(self._worker_class.process,kwds=dict(
                input_tube=self._input_tube,
                output_tubes=self._output_tubes,
                index=i,
                num_workers=self._num_worker,
                disable_result=self._disable_result,
                worker_args=self._worker_args,
                name=self.name,
                show_time=self.show_time))
        # self.workers_pool.map_async(_worker_process2,range(self._num_worker))
        self.workers_pool.close()
        # self.workers_pool.join()
        # self.workers_pool.
                # Build all downstream stages.
        for stage in self._next_stages:
            stage.build()
        self.available_output_tubes = list(self._output_tubes)

    
    