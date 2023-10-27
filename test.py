from pandas import DataFrame

from parallframe import Splitter
from parallframe import ParallelProcessor

from custom_functions import do_high_perfomance

from random import randint, uniform
from time import time
from functools import partial


def benchmark(function, *args, **kwargs):
    time_start = time()
    function(*args, **kwargs)
    print(f"[TEST] {time() - time_start} seconds")


def test(df, workers=2):
    splitter = Splitter(workers=workers, ignore_dataframe_size=False)
    processor = ParallelProcessor(splitter, do_high_perfomance)

    result_df = processor.parallelize_dataframe(df)
    print(result_df)


if __name__ == '__main__':
    data = {
        'A': [randint(100000, 100000000) for _ in range(1000000)],
        'B': [uniform(100000.0, 10000000.0) for _ in range(1000000)]
    }
    df = DataFrame(data)
    for workers in range(1, 7):
        benchmark(partial(test, df=df, workers=workers))

