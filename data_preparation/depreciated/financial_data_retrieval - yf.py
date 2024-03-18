from utilities.util import save_file
import yfinance as yf
import pandas as pd


class FinancialDataRetriever:
    def __init__(self, tickers):
        self.tickers = tickers
        self.all_data = []

    def fetch_data(self):
        for ticker in self.tickers:
            stock = yf.Ticker(ticker)

            # Fetch financial statement data
            income_stmt = stock.get_income_stmt(freq='quarterly')
            balance_sheet = stock.get_balance_sheet(freq='quarterly')
            cash_flow = stock.get_cash_flow(freq='quarterly')

            # Append the data to the list
            self.all_data.append({
                'Ticker': ticker,
                'Income Statement': income_stmt,
                'Balance Sheet': balance_sheet,
                'Cash Flow': cash_flow
            })

            # Save data as CSV
            income_stmt.to_csv(f'data/financial_data/{ticker}_income_stmt.csv')
            balance_sheet.to_csv(f'data/financial_data/{ticker}_balance_sheet.csv')
            cash_flow.to_csv(f'data/financial_data/{ticker}_cash_flow.csv')

stock.get_income_stmt(freq='quarterly', as_dict=True).keys()
# # Example usage
# tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

# data_retriever = FinancialDataRetriever(tickers)


# earnings_calendar = pdr.get_data_yahoo([ticker])
# earnings_calendar
# from yahoo_earnings_calendar import YahooEarningsCalendar

# yec = YahooEarningsCalendar()
#     # Returns a list of all available earnings of BOX
# print(yec.get_earnings_of('AAPL'))

