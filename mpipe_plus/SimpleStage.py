"""Implements UnorderedStage class."""

from .Stage import Stage
from .Worker import Worker
from .tube_q import TubeQ

__all__ = ['SimpleStage']


class _Worker(Worker):
    def __init__(self, task_fn):
        super(_Worker, self).__init__()
        self.task_fn = task_fn

    def doTask(self, task):
        return self.task_fn(task)

    def __str__(self):
        return self.task_fn.__name__


class SimpleStage(Stage):
    """A specialized :class:`~mpipe.Stage`, 
    internally creating :class:`~mpipe.UnorderedWorker` objects."""
    def __init__(self, target, num_worker=1, disable_result=False, max_backlog=None,multi_process=True,show_time=False):
        """Constructor takes a function implementing
        :meth:`UnorderedWorker.doTask`."""
        super(SimpleStage, self).__init__(_Worker, num_worker, multi_process,disable_result,
                                             input_tube=TubeQ(maxsize=max_backlog) if max_backlog else None,show_time=show_time,
                                             task_fn=target)
