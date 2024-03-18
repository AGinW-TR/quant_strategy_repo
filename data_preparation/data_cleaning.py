import warnings

from utilities.util import load_config, save_file, log
import pandas as pd
import os 
from tqdm import tqdm 


warnings.filterwarnings("ignore")


def read_eps_data():
    """
    Read EPS data from multiple files and return a DataFrame.

    Returns:
        df_eps (pandas.DataFrame): DataFrame containing EPS data.
    """
    log('Reading EPS data')
    eps_files = os.listdir('data/eps_data')
    df_eps = pd.DataFrame()
    for eps_file in eps_files:
        df_temp = pd.read_csv('data/eps_data/' + eps_file, 
                              parse_dates=['Event Start Date'],
                              usecols=['Symbol', 'Event Start Date', 'EPS Estimate', 'Reported EPS', 'Surprise (%)'])
        df_eps = pd.concat((df_eps, df_temp))

    df_eps['Event Start Date'] = df_eps['Event Start Date'].dt.date
    return df_eps


def read_market_data(config):
    """
    Read market data from a file and return a DataFrame.

    Returns:
        df_market (pandas.DataFrame): DataFrame containing market data.
    """
    log('Reading market data')
    start_date = config['start_date']
    end_date = config['end_date']
    market_data_file = f'data/market_data/{start_date} to {end_date}.csv'

    df_market = pd.read_csv(market_data_file, 
                              parse_dates=['Date'],
                              usecols=['Date', 'Ticker', 'Adj Close', 'Volume'])
    df_market['Date'] = df_market['Date'].dt.date

    return df_market


def filter_by_tickers(df):
    log('Filtering by tickers')
    selected_tickers = pd.read_csv('data/market_data/selected_tickers.csv')['Ticker'].tolist()
    df = df.query('Ticker in @selected_tickers')
    return df 


def drop_outliers(df, config):
    """
    Drop outliers from the input DataFrame and return the cleaned DataFrame.

    Args:
        df (pandas.DataFrame): Input DataFrame.

    Returns:
        cleaned_df (pandas.DataFrame): Cleaned DataFrame.
    """

    log('Dropping outliers')

    targets = config['targets']
    lower_bound, upper_bound = config['target_outlier_threshold']

    for target in targets:
        df = df.query(f'{target} > {lower_bound} and {target} < {upper_bound}')

    return df


def generate_target(market_data_ticker, eps_data_ticker):
    merged_data = pd.merge(market_data_ticker,
                                eps_data_ticker,
                                left_on='Date',
                                right_on='Event Start Date',
                                how='left')
        
    # Initialize the target variables
    merged_data['period_max_price'] = pd.NA
    merged_data['period_min_price'] = pd.NA

    # Extract dates with EPS data for target calculation
    eps_dates = merged_data.dropna(subset='Surprise (%)')['Event Start Date'].unique()

    # Loop through the EPS dates to calculate the targets
    for i in range(len(eps_dates) - 1):
        prev_release_date = eps_dates[i]
        release_date = eps_dates[i + 1]

        # Find the max and min 'Adj Close' between the two EPS dates
        period_data = market_data_ticker[(market_data_ticker['Date'] > prev_release_date) & (market_data_ticker['Date'] < release_date)]
        period_max_price = period_data['Adj Close'].max()
        period_min_price = period_data['Adj Close'].min()

        # Update the targets in the merged dataset
        merged_data.loc[merged_data['Event Start Date'] == prev_release_date, ['period_max_price', 'period_min_price']] = (period_max_price, period_min_price)
    return merged_data


def calculate_targets_for_single_ticker(ticker):
    # Step 1: Query market data for the current ticker
    market_data_ticker = df_market[df_market['Ticker'] == ticker]

    # Step 2: Read current ticker EPS data
    eps_data_ticker = df_eps[df_eps['Symbol'] == ticker]

    # This will require your logic to determine the periods and calculate the max/min prices
    merged_data = generate_target(market_data_ticker, eps_data_ticker)
    return merged_data  # This will include your target calculations


def calculate_targets_for_all_tickers(df_market, df_eps):
    """
    Calculate the target variables for each ticker in the dataset.

    Args:
        df_market (pandas.DataFrame): DataFrame containing market data.
        df_eps (pandas.DataFrame): DataFrame containing EPS data.

    Returns:
        df (pandas.DataFrame): DataFrame containing the target variables.
    """

    log('Calculating targets')
    tickers = df_market['Ticker'].unique()
    target_data = pd.DataFrame()
    for ticker in tqdm(tickers):
        target_data = pd.concat([target_data, calculate_targets_for_single_ticker(ticker)])
    target_data.drop(columns=['Symbol', 'Event Start Date'], inplace=True)

    print(target_data.shape)
    print('# of unique Ticker:', target_data['Ticker'].nunique())
    return target_data

# Example usage
if __name__ == '__main__':
    config = load_config()
    df_eps = read_eps_data()
    df_market = read_market_data(config)

    df_market = filter_by_tickers(df_market)
    merged_data = calculate_targets_for_all_tickers(df_market, df_eps)

    merged_data = drop_outliers(merged_data, config)
    save_file(merged_data, 'data/processed_data/data_clean.csv', index=False)


