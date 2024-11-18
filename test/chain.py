import logging
import random
import time
from mpipe_plus import ( Pipeline, SimpleStage)
from mpipe_plus.work_exception import WorkException


def increment(value):
    # print("=",value)
    time.sleep(1.2)
    return value + 1


def double(value):
    # print("+",value)
    # raise Exception("DD")
    time.sleep(1.2)
    # time.sleep(2)
    return value * 2


def echo(value):
    time.sleep(1)
    return value


def main():
    workers=100
    multi_process=0
    stage1 = SimpleStage(increment,workers,multi_process=multi_process,show_time=False)
    stage2 = SimpleStage(double,workers,multi_process=multi_process,show_time=False)
    stage3 = SimpleStage(echo,workers,multi_process=multi_process,show_time=False)
    stage1.link(stage2)
    stage2.link(stage3)
    pipe = Pipeline(stage1)

    # for number in range(1,10):
    #     pipe.put(number)

    # pipe.put(StopIteration())


    for result in pipe.run(range(0,10),ordered_results=True):
        if isinstance(result,WorkException):
            result.re_raise()
        print(result)


if __name__ == '__main__':
    s=time.time()
    main()
    print("total time:",time.time()-s)
