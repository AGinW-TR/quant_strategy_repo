"""
download market data from yahoo finance to local
"""
import json 
import yfinance as yf
import os
import pandas as pd
from tqdm import tqdm
from utilities.util import log, load_config, save_file
from urllib.request import urlretrieve
import requests
import pandas as pd
import json
from tqdm import tqdm


class MarketDataDownloader:
    def __init__(self, config_path='config.yaml'):
        self.config = load_config(config_path)
        self.data_dir = self.config['data_dir']
        self.start_date = self.config['start_date']
        self.cutoff_date = self.config['cutoff_date']
        self.end_date = self.config['end_date']

        self.tickers = None
        self.data = None

    def fetch_and_save_nasdaq_ticker_list(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        tickers_file_path = os.path.join(self.data_dir, 'nasdaq_tickers.csv')

        if os.path.exists(tickers_file_path):
            log('Ticker list already downloaded')
            return

        log(f"Downloading NASDAQ ticker list to {tickers_file_path}")
        url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
        urlretrieve(url, tickers_file_path)
        log(f"NASDAQ ticker list saved to {tickers_file_path}")

    def read_nasdaq_ticker_list(self):
        tickers_file_path = os.path.join(self.data_dir, 'nasdaq_tickers.csv')
        tickers_df = pd.read_csv(tickers_file_path)
        tickers = tickers_df['Symbol'].tolist()
        tickers = list(filter(lambda x: isinstance(x, str), tickers))
        self.tickers = tickers#[:10]
        log(f'{len(tickers)} tickers loaded')

    def download_market_data(self):
        tickers = self.tickers
        filename = os.path.join(self.data_dir, self.start_date + ' to ' + self.end_date + '.csv')
        if os.path.exists(filename):
            log(f'File {filename} already exists, loading data from file...')
            self.data = pd.read_csv(filename)
            df_selected_tickers = df_trade.query('Date == "2024-01-04"').dropna(subset='Volume').query('Volume > 10000').query('Volume * `Adj Close` > 500000').Ticker.to_frame()
            save_file(data=df_selected_tickers, file_path='data/market_data/selected_tickers.csv', index=False)
            self.selected_tickers = df_selected_tickers.Ticker.tolist()
            return self.data
        
        log(f'Downloading market data from {self.start_date} to {self.end_date}...')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        data = yf.download(' '.join(tickers), 
                           start=self.start_date, 
                           end=self.end_date)

        log(f'Downloading completed, saving file to {filename}...')
        
        data.index = data.index.map(lambda x: str(x.date()))

        # put in better format 
        data = data.reset_index().melt(id_vars='Date')
        data = data.pivot(index=['Ticker', 'Date'], columns=['Price']).reset_index()
        data.columns = data.columns.map(lambda x: x[-1] if x[-1] else x[0])
        save_file(data=data, file_path=filename, index=False)
        self.data = data

        return self.data    


    def fetch_earnings_data(self):
        log('Fetching earnings data...')
        tickers = self.selected_tickers
        url = "https://query1.finance.yahoo.com/v1/finance/visualization?crumb=3ytadF1OTRB&lang=en-US&region=US&corsDomain=finance.yahoo.com"

        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cookie": "GUCS=AWwtLxNI; GUC=AQEBCAFl9bpmHkIlogVH&s=AQAAAGIX1SIq&g=ZfRrrQ; A1=d=AQABBJlqMWUCEEBt8wa3N2VSTB9-Z4P79vwFEgEBCAG69WUeZtxS0iMA_eMBAAcImWoxZYP79vw&S=AQAAAlnBlbgWYsFV2MLbBMKB-nw; A3=d=AQABBJlqMWUCEEBt8wa3N2VSTB9-Z4P79vwFEgEBCAG69WUeZtxS0iMA_eMBAAcImWoxZYP79vw&S=AQAAAlnBlbgWYsFV2MLbBMKB-nw; A1S=d=AQABBJlqMWUCEEBt8wa3N2VSTB9-Z4P79vwFEgEBCAG69WUeZtxS0iMA_eMBAAcImWoxZYP79vw&S=AQAAAlnBlbgWYsFV2MLbBMKB-nw; cmp=t=1710517161&j=0&u=1YNN; gpp=DBAA; gpp_sid=-1; axids=gam=y-jXntMQhE2uJaNY8CIx0vM0p.vlt8Dskg~A&dv360=eS02UWVlWERsRTJ1RkJLTFQ5X0JydUZPWHBVVGtraEZOTX5B&ydsp=y-T8qMqydE2uLC39n6fZJekRl8ArXdXN1G~A&tbla=y-qoCbAp9E2uIDyjs4hFkaoq3wUmcaknDi~A; tbla_id=1f584ffc-d82e-405f-9672-fd860204bcdc-tuctc2af5d6; __gpi=UID=00000dc7e3c403f0:T=1710517174:RT=1710517174:S=ALNI_Ma3ipvAvSYxlYgzdAcIrUxxhQw1Jw; __eoi=ID=d89798e78dce9cb3:T=1710517174:RT=1710517174:S=AA-AfjaxgYtlwVUfOb69qYuDvxVQ",
            "Origin": "https://finance.yahoo.com",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        df_eps = pd.DataFrame()

        for ticker in tqdm(tickers):
            payload = {
                "sortType": "DESC",
                "entityIdType": "earnings",
                "sortField": "startdatetime",
                "includeFields": [
                    "ticker", "companyshortname", "eventname", "startdatetime",
                    "startdatetimetype", "epsestimate", "epsactual", "epssurprisepct",
                    "timeZoneShortName", "gmtOffsetMilliSeconds"
                ],
                "offset": 0,
                "query": {
                    "operator": "or",
                    "operands": [
                        {"operator": "eq", "operands": ["ticker", ticker]}
                    ]
                },
                "size": 100
            }

            response = requests.post(url, headers=headers, data=json.dumps(payload))

            if response.status_code == 200:
                data = response.json()
                columns = data['finance']['result'][0]['documents'][0]['columns']
                columns = [x['label'] for x in columns]

                df_temp = pd.DataFrame(data['finance']['result'][0]['documents'][0]['rows'], columns=columns)
                save_file(data=df_temp, file_path=f"data/eps_data/{ticker}.csv", index=False)
                # df_eps = pd.concat((df_eps, df_temp))

            else:
                log(f"Failed to retrieve data for {ticker}, status code:", response.status_code)
        
        # self.df_eps = df_eps
        # return df_eps

if __name__ == "__main__":
    downloader = MarketDataDownloader()  # Initialize downloader with default config path
    
    # Ensure the NASDAQ ticker list is fetched and saved locally
    downloader.fetch_and_save_nasdaq_ticker_list()
    
    # Read the NASDAQ tickers from the local file
    downloader.read_nasdaq_ticker_list()
    
    # Download market data for the first 10 tickers as a sample
    
    df_trade = downloader.download_market_data()

    # Fetch earnings data for the first 10 tickers as a sample
    downloader.fetch_earnings_data()



