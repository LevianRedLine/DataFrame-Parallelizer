from pandas import DataFrame

from parallframe import Splitter
from parallframe import ParallelProcessor

from custom_functions import custom_function

from random import randint, uniform


if __name__ == "__main__":
    data = {
        'A': [randint(1, 100) for _ in range(10000)],
        'B': [uniform(1.0, 100.0) for _ in range(10000)]
    }
    df = DataFrame(data)

    # Создаем экземпляры классов
    splitter = Splitter(workers=2, ignore_dataframe_size=False)
    processor = ParallelProcessor(splitter, custom_function)

    # Выполняем параллельную обработку
    result_df = processor.parallelize_dataframe(df)
    print(result_df)
