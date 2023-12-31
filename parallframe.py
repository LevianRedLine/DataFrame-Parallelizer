"""
Этот модуль содержит классы для реализации многопоточной (мультипроцессорной) обработки Датафреймов.
"""

from pandas import DataFrame
from pandas import concat
from numpy import array_split

from functools import singledispatchmethod
from concurrent.futures import ProcessPoolExecutor
from types import FunctionType


class Splitter:
    """
    Класс 'Splitter' реализует задачу разделения датафрейма на поддатафреймы.

    Параметры:
    - workers (int) - Количество рабочих процессов для обработки данных.
    Подразумевается, что количество процессов будет равно количеству поддатафреймов.
    - ignore_dataframe_size (bool) - Игнорирует размер датафрейма.
    Если True, то Датафрейм будет выполняться в одном процессе.


    Пример использования:
    splitter = Splitter(workers=2, ignore_dataframe_size=True)
    split_dataframe = splitter.split_dataframe(df)
    split_dataframe() вернет список из поддатафреймов. Можно использовать напрямую в своих целях.

    Примечание:
    Если нет необходимости параллельной обработки, рекомендуется использовать встроенную функцию
    модуля 'numpy' array_split(). Этот класс предназначен для использования внутри класса ParallelProcessor
    """
    def __new__(cls, workers: int = 1, ignore_dataframe_size: bool = False):
        """
        Проверка на валидность параметров
        """
        if not isinstance(workers, int):
            raise TypeError(f"parameter 'workers' must be int type. You worker is {type(workers)} type")
        if 0 >= workers > 61:
            raise ValueError(f"Parameter 'workers' must be greater than 0 and lover than 62. Workers = {workers}")
        if not isinstance(ignore_dataframe_size, bool):
            raise TypeError(f"Parameter 'ignore_dataframe_size' must be bool type. "
                            f"Your ignore_dataframe_size is {type(ignore_dataframe_size)} type")
        return super().__new__(cls)

    def __init__(self, workers, ignore_dataframe_size):
        self.workers = workers
        self.ignore_dataframe_size = ignore_dataframe_size

    @singledispatchmethod
    def split_dataframe(self, dataframe: DataFrame) -> (TypeError | Exception):
        """
        Разделяет датафрейм на поддатафреймы для обработки.

        Параметры:
        - dataframe (DataFrame): Датафрейм, который необходимо разделить.

        Возвращает:
        - list[DataFrame]: Список поддатафреймов, готовых для обработки.

        Исключения:
        - TypeError: Если параметр 'dataframe' не является типом DataFrame.
        - Exception: Если параметр 'ignore_dataframe_size' равен False,
        и количество процессов больше, чем размер датафрейма.

        Пример использования:
        splitter = Splitter(workers=2, ignore_dataframe_size=False)
        dataframes = splitter.split_dataframe(df)
        """
        raise TypeError(f'Dataframe parameter must be DataFrame type. Your dataframe is {type(dataframe)} type')

    @split_dataframe.register(DataFrame)
    def _split_dataframe(self, dataframe):
        if self.ignore_dataframe_size:
            return [dataframe]
        else:
            if len(dataframe) < self.workers:
                raise Exception("Count of workers must be greater than lenght of dataframe object")
            else:
                return array_split(dataframe, self.workers)


class ParallelProcessor:
    """
    Класс ParallelProcessor реализует параллельную обработку набора датафреймов.

    Параметры:
    - splitter (Splitter): Объект класса Splitter для разделения датафреймов.
    - function (FunctionType): Пользовательская функция для обработки датафреймов.

    Пример использования:
    processor = ParallelProcessor(splitter, custom_function)
    """
    def __new__(cls, splitter: Splitter, function: FunctionType):
        if not isinstance(splitter, Splitter):
            raise TypeError(f"Parameter 'splitter' must be Splitter type. Your splitter is {type(splitter)} type")
        if not isinstance(function, FunctionType):
            raise TypeError(f"Parameter 'function' must be FunctionType. Your function is {type(function)} type")
        return super().__new__(cls)

    def __init__(self, splitter, function):
        self.splitter = splitter  # Сплиттер, для разделения датафрейма на поддатафреймы
        self.function = function  # Функция, которая должна обрабатывать датафрейм

    @singledispatchmethod
    def parallelize_dataframe(self, dataframe: DataFrame) -> TypeError:
        """
        Используя объект класса Splitter разделяет датафрейм и запускает параллельную обработку.

        Параметры:
        - dataframe (DataFrame): Датафрейм, который необходимо обработать

        Возвращает:
        - combined_df (DataFrame): Обработанный датафрейм

        Исключения:
        - TypeError: Если параметр 'dataframe' не является типом DataFrame.
        """
        raise TypeError(f"Parameter 'dataframe' must be DataFrame type.")

    @parallelize_dataframe.register(DataFrame)
    def _parallelize_dataframe(self, dataframe: DataFrame) -> DataFrame:
        dataframe_split = self.splitter.split_dataframe(dataframe)  # Разделение датафрейма
        workers = len(dataframe_split)  # Определение количества поток исходя из размера датафрейма
        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Параллельная обработка датафрейма, с применением функции self.function()
            results = list(executor.map(self.function, dataframe_split))
        combined_df = concat(results)  # Объединение обработанных поддатафреймов
        return combined_df
