from utilities.util import log


def calculate_moving_averages(df, window_sizes=[5, 10, 20]):
    """
    Calculate moving averages for given window sizes.

    :param df: Pandas DataFrame with market data
    :param window_sizes: List of integers for window sizes
    :return: DataFrame with moving averages added as new columns
    """
    log(f'calculating moving averages for window_sizes {window_sizes}')

    for window in window_sizes:
        ma_col = 'ma_{}'.format(window)
        df[ma_col] = df.groupby('Ticker')['Adj Close']\
                       .rolling(window=window)\
                       .mean()
    return df


def calculate_rsi(df, period=14):
    """
    Calculate Relative Strength Index (RSI).

    :param df: Pandas DataFrame with market data
    :param period: Period for calculating RSI
    :return: DataFrame with RSI added as a new column
    """
    log(f'calculating rsi for period {period}')

    delta = df.groupby('Ticker')['Adj Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    return df


def calculate_n_day_return(data, n=7, price_column='Adj Close'):
    """
    Calculate the 7-day return for the given price column.

    Parameters:
    - data: DataFrame containing the historical price data.
    - price_column: The name of the column containing the price data.

    Returns:
    - data: DataFrame with an additional column for the 7-day return.
    """
    data[f'{n}_day_return'] = data.groupby('Ticker')[price_column]\
                                  .pct_change(periods=n)\
                                  .shift(-n)
    return data


def get_lag(data, columns, n_lag=7):
    log(f'getting {n_lag} days lag for columns {columns}')
    for col in columns:
        for i in range(1, n_lag+1):
            data[f'{col}_lag_{i}'] = data.groupby('Ticker')[col].shift(i)

    return data


def binarlizer(data, categorecal_features):
    log('binaralizing categorecal_features')
    for col in categorecal_features:
        data[col] = data[col].astype('category')
    return data


def normalize_price(data):
    log('normalizing price')
    data['pct_change'] = data.groupby('Ticker')['Adj Close'].pct_change() * 100
    data['period_max_price_pct'] = (data['period_max_price'] /
                                    data['Adj Close'] * 100) - 100
    data['period_min_price_pct'] = (data['period_min_price'] /
                                    data['Adj Close'] * 100) - 100

    return data
