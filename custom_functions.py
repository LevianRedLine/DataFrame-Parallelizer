from pandas import DataFrame


def custom_function(df: DataFrame) -> DataFrame:
    df['A'] = df['A'].apply(lambda n: n > 1 and all(n % i != 0 for i in range(2, int(n ** 0.5) + 1)))
    df['B'] = df['B'] ** 10
    return df


def do_high_perfomance(df: DataFrame) -> DataFrame:
    df['A'] = df['A'].apply(lambda n: n > 1 and all(n % i != 0 for i in range(2, int(n ** 0.5) + 1)))
    df['B'] = (df['B'] ** 0.5) ** 0.6 / 7777777 * 6666666
    return df