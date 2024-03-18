import requests
import pandas as pd
import json

def fetch_earnings_data(symbols):
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
    
    df_res = pd.DataFrame()

    for symbol in symbols:
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
                    {"operator": "eq", "operands": ["ticker", symbol]}
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
            df_res = pd.concat((df_res, df_temp))

        else:
            print(f"Failed to retrieve data for {symbol}, status code:", response.status_code)
    
    return df_res

# Example usage:
symbols = ["AMZN", "AAPL"]  # Add more symbols as needed
df = fetch_earnings_data(symbols)
print(df)

