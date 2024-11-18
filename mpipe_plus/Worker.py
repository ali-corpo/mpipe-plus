"""Implements UnorderedWorker class."""

import abc
import multiprocessing
from typing import Generic, TypeVar

from mpipe_plus.tube_p import TubeP

from .tube import Tube
from .timer import Timer
from .work_exception import WorkException
from .tube_q import TubeQ

T = TypeVar('T')
Q = TypeVar('Q')

class Worker(abc.ABC, Generic[T, Q]):
    """An UnorderedWorker object operates independently of other
    workers in the stage, fetching the first available task, and
    publishing its result whenever it is done
    (without coordinating with neighboring workers).
    Consequently, the order of output results may not match 
    that of corresponding input tasks."""

    def __init__(self):
        pass

    def init2(
        self, 
        input_tube:Tube[tuple[tuple[int,T|BaseException],int]],      # Read task from the input tube.
        output_tubes:list[Tube[tuple[tuple[int,Q|BaseException],int]]],    # Send result on all the output tubes.
        num_workers:int,     # Total number of workers in the stage.
        disable_result:bool,  # Whether to override any result with None.
        name:str,
        index:int,
        show_time:bool
        ):
        """Create *num_workers* worker objects with *input_tube* and 
        an iterable of *output_tubes*. The worker reads a task from *input_tube* 
        and writes the result to *output_tubes*."""

        super(Worker, self).__init__()
        self.show_time=show_time
        self.name=name
        self.index=index
        self.process_executed={
            "doInit":Timer("init"),
            "doTask":Timer("perTask",per_item=True),
            "doDispose":Timer("dispose"),
            "task_get_input":Timer("avg_in_wait",per_item=True),
            "task_put_output":Timer("avg_out_wait",disable=True,per_item=True),
        }
        self._tube_task_input = input_tube
        self._tubes_result_output = output_tubes
        self._num_workers = num_workers
        self._disable_result = disable_result
        
    @staticmethod
    def getTubeClass():
        """Return the tube class implementation."""
        return TubeP[tuple[tuple[int,T],int]|None]
    


    def putResult(self, task_index, result:Q|BaseException):
        """Register the *result* by putting it on all the output tubes."""
        for tube in self._tubes_result_output:
            tube.put(((task_index,result), 0))

    @classmethod
    def process(cls,worker_args,**kwargs):
        try:
            instance=cls(**worker_args)
            instance.init2(**kwargs)
            instance.run()
        except Exception as e:
            print("Exception in worker init:",e)
            import traceback
            traceback.print_exception(e)
            raise

    def run(self):
        try:
            # Run implementation's initialization.
            with self.process_executed["doInit"]:
                self.doInit()
        
            while True:
                try:
                    (task_index,task), count = self._tube_task_input.get()
                except Exception as e:
                    (task_index,task), count = ((-1,e), 0)

                # In case the task is None, it represents the "stop" request,
                # the count being the number of workers in this stage that had
                # already stopped.
                if isinstance(task, StopIteration) :
                    # If this worker is the last one (of its stage) to receive the 
                    # "stop" request, propagate "stop" to the next stage. Otherwise,
                    # maintain the "stop" signal in this stage for another worker that
                    # will pick it up. 
                    count += 1
                    if count == self._num_workers:
                        self.putResult(task_index,task)
                    else:
                        self._tube_task_input.put(((task_index,task), count))

                    # In case we're calling doTask() on a "stop" request, do so now.
                
                    # Honor the "stop" request by exiting the process.
                    break  
                if isinstance(task, WorkException):
                    self.putResult(task_index,task)
                    self.close_pipes()
                    break
                if isinstance(task, BaseException):
                    self._tube_task_input.put(((task_index,task), count+1))
                    raise task
                # The task is not None, meaning that it is an actual task to
                # be processed. Therefore let's call doTask().
                with self.process_executed["doTask"]:
                    try:
                        result = self.doTask(task)
                    except Exception as e:
                        self.putResult(task_index,WorkException(e, self, task))
                        self.close_pipes()
                        break

                # Unless result is disabled,
                # if doTask() actually returns a result (and the result is not None),
                # it indicates that it did not call putResult(), instead intending
                # it to be called now.
                if not self._disable_result and result is not None:
                    with self.process_executed['task_put_output']:
                        self.putResult(task_index,result)

        except KeyboardInterrupt as e:
            self.putResult(-1,e)
        except Exception as e:
            self.putResult(-1,WorkException(e, self, None))
            self.close_pipes()
        finally:
            with self.process_executed['doDispose']:
                self.doDispose()
            if self.show_time:
                # avg_out_wait:{stats['task_put_output']/stats['task_count']:.2f}s
                stats=" ".join(f"{str(v):>30}" for k,v in self.process_executed.items() if v.elapsed_time>.001)
                print(f"[{self.index}]{str(self):<10}: {stats}")

    def close_pipes(self):
        self._tube_task_input.close()
        for tube in self._tubes_result_output:
            tube.close()

    @abc.abstractmethod
    def doTask(self, task:T)->Q:
        """Implement this method in the subclass with work
        to be executed on each *task* object.
        The implementation can publish the output result in one of two ways,
        either by 1) calling :meth:`putResult` and returning ``None``, or
        2) returning the result (other than ``None``)."""
        

    def doInit(self):
        """Implement this method in the subclass in case there's need
        for additional initialization after process startup.
        Since this class inherits from :class:`multiprocessing.Process`,
        its constructor executes in the spawning process.
        This method allows additional code to be run in the forked process,
        before the worker begins processing input tasks.
        """
        return None


    def doDispose(self):
        """Implement this method in the subclass in case there's need
        for additional cleanup after process termination.
        """
        return None

    def __str__(self):
        return self.name