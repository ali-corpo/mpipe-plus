"""Implements OrderedWorker class."""

import abc
import logging
import multiprocessing
from multiprocessing.synchronize import Lock
import time
from typing import Generic, TypeVar

from .work_exception import WorkException

from .timer import Timer

from .tube import Tube
from .tube_p import TubeP

T = TypeVar('T')
Q = TypeVar('Q')

class OrderedWorker(multiprocessing.Process,abc.ABC, Generic[T, Q]):
    """An OrderedWorker object operates in a stage where the order 
    of output results always matches that of corresponding input tasks.

    A worker is linked to its two nearest neighbors -- the previous 
    worker and the next -- all workers in the stage thusly connected
    in circular fashion. 
    Input tasks are fetched in this order. Before publishing its result, 
    a worker first waits for its previous neighbor to do the same."""

    def __init__(self) -> None:
        pass

    def init2(
        self, 
        input_tube:Tube[tuple[T|BaseException,int]],      # Read task from the input tube.
        output_tubes:list[Tube[tuple[Q|BaseException,int]]],    # Send result on all the output tubes.
        num_workers:int,     # Total number of workers in the stage.
        disable_result:bool,  # Whether to override any result with None.
        # do_stop_task:bool,    # Whether to call doTask() on "stop" request.
        name:str,
        index:int
        ):
        """Create *num_workers* worker objects with *input_tube* and 
        an iterable of *output_tubes*. The worker reads a task from *input_tube* 
        and writes the result to *output_tubes*."""

        super(OrderedWorker, self).__init__()
        self.process_executed={
            "doInit":Timer("init"),
            "doTask":Timer("perTask",per_item=True),
            "doDispose":Timer("dispose"),
            "task_get_input":Timer("avg_in_wait",per_item=True),
            "task_put_output":Timer("avg_out_wait",disable=True,per_item=True),
        }
        self.index=index
        self._tube_task_input = input_tube
        self._tubes_result_output = output_tubes
        self._num_workers = num_workers

        # Serializes reading from input tube.
        self._lock_prev_input:Lock|None = None
        self._lock_next_input:Lock|None = None

        # Serializes writing to output tube.
        self._lock_prev_output:Lock|None = None
        self._lock_next_output:Lock|None = None

        self._disable_result = disable_result
        self._do_stop_task = False
        self.worker_name=name
    @staticmethod
    def getTubeClass():
        """Return the tube class implementation."""
        return TubeP[tuple[tuple[int,T],int]|None]

    @classmethod
    def assemble(
        cls, 
        args, 
        input_tube, 
        output_tubes, 
        size, 
        disable_result=False,
        # do_stop_task=False,
        name:str=""
        ):
        """Create, assemble and start workers.
        Workers are created of class *cls*, initialized with *args*, and given
        task/result communication channels *input_tube* and *output_tubes*.
        The number of workers created is according to *size* parameter.
        *do_stop_task* indicates whether doTask() will be called for "stop" request.
        """

        # Create the workers.
        workers = []
        for ii in range(size):
            worker = cls(**args)
            worker.init2(
                input_tube,
                output_tubes,
                size,
                disable_result,
                # do_stop_task,
                name=name,
                index=ii
                )
            workers.append(worker)

        # Connect the workers.
        for ii in range(size):
            worker_this = workers[ii]
            worker_prev = workers[ii-1]
            worker_prev._link(
                worker_this, 
                next_is_first=(ii==0),  # Designate 0th worker as the first.
                )

        # Start the workers.
        for worker in workers:
            worker.start()

    def _link(self, next_worker, next_is_first=False):
        """Link the worker to the given next worker object, 
        connecting the two workers with communication tubes."""

        lock: Lock = multiprocessing.Lock()
        next_worker._lock_prev_input = lock
        self._lock_next_input = lock
        lock.acquire()

        lock = multiprocessing.Lock()
        next_worker._lock_prev_output = lock
        self._lock_next_output = lock
        lock.acquire()

        # If the next worker is the first one, trigger it now.
        if next_is_first:
            self._lock_next_input.release()
            self._lock_next_output.release()

    def putResult(self, result:Q|BaseException,lock=True):
        """Register the *result* by putting it on all the output tubes."""
        assert self._lock_prev_output
        assert self._lock_next_output
        if lock:
            self._lock_prev_output.acquire()
        
        for tube in self._tubes_result_output:
            tube.put((result,0))
        if lock:
            self._lock_next_output.release()
        
    def run(self):
        assert self._lock_prev_input
        assert self._lock_next_input
        # Run implementation's initialization.
        with self.process_executed['doInit']:
            self.doInit()
        
        
        try:
            while True:
                try:
                    # Wait on permission from the previous worker that
                    # it is okay to retrieve the input task.
                    with self.process_executed['task_get_input']:
                        self._lock_prev_input.acquire()

                        # Retrieve the input task.
                        (task, count) = self._tube_task_input.get()

                        # Permit the next worker to retrieve the input task.
                        self._lock_next_input.release()
                except Exception    as e:
                    print(e)
                    (task, count) = (e, 0)


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
                        # Propagating the "stop" to the next stage does not require
                        # synchronization with previous and next worker because we're
                        # guaranteed (from the count value) that this is the last worker alive. 
                        # Therefore, just put the "stop" signal on the result tube.
                        self.putResult(task,lock=False)#maybe without lock
                    else:
                        self._tube_task_input.put((task, count))

                    # # In case we're calling doTask() on a "stop" request, do so now.
                    # if self._do_stop_task:
                    #     self.doTask(None)

                    # Honor the "stop" request by exiting the process.
                    break  
                if isinstance(task, BaseException):
                    self._tube_task_input.put((task, count+1))
                    raise task
                    
                    
                # The task is not None, meaning that it is an actual task to
                # be processed. Therefore let's call doTask().
                
                with self.process_executed['doTask']:

                    try:
                        result = self.doTask(task)
                    except Exception as e:
                        raise WorkException(e, self, task)
        
                # Unless result is disabled,
                # if doTask() actually returns a result (and the result is not None),
                # it indicates that it did not call putResult(), instead intending
                # it to be called now.
                if not self._disable_result and result is not None:
                    with self.process_executed['task_put_output']:
                        self.putResult(result)
                    
                
        except WorkException as e:
            self.putResult(e)
            self.close_pipes()
        except KeyboardInterrupt as e:
            self.putResult(e)
        except Exception as e:
            self.putResult(WorkException(e, self, None))
            self.close_pipes()
        finally:
            with self.process_executed['doDispose']:
                self.doDispose()
            
            # avg_out_wait:{stats['task_put_output']/stats['task_count']:.2f}s
            stats=" ".join(f"{str(v):>30}" for k,v in self.process_executed.items() if v.elapsed_time>.001)
            print(f"[{self.index}]{str(self):<10}: {stats}")

    def close_pipes(self):
        self._tube_task_input.close()
        for tube in self._tubes_result_output:
            tube.close()

    @abc.abstractmethod
    def doTask(self, task:T)->Q:
        """Implement this method in the subclass with work functionality
        to be executed on each *task* object.
        The implementation can publish the output result in one of two ways,
        either by 1) calling :meth:`putResult` and returning ``None``, or
        2) returning the result (other than ``None``)."""
        pass    

    
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
        return self.worker_name