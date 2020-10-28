import os
import random
import pandas as pd
import requests

from master.get_BHAV_FILE import get_Bhav_file
Stoc_le=[]

def make_dataframe(s, StockSymbol, date):
    # From this python file we are collecting companyName, pdSectorInd, totalMarketCap
    result=[]
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"
    }
    r_stock = s.get('https://www.nseindia.com/api/quote-equity?symbol=' + StockSymbol,
                    headers=headers).json()

    r_mcp = s.get(
        'https://www.nseindia.com/api/quote-equity?symbol=' + StockSymbol + '&section=trade_info',
        headers=headers).json()
    tag = None
    try:
        if r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 1000:
            tag = "VSM"
        elif 1000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 7000:
            tag = "Small"
        elif 7000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 28000:
            tag = "Mid"
        elif 28000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100:
            tag = "LARGE"
    except:
        tag = None

    try:
        result.append(StockSymbol)
    except Exception as e:
        print(e,"StockSymbol")
        result.append(None)

    try:
        result.append(r_stock['info']['companyName'])
    except Exception as e:
        print(e,"companyName")
        result.append(None)


    try:
        result.append(r_stock['metadata']['pdSectorInd'].rstrip())
    except Exception as e:
        print(e,"pdSectorInd")
        result.append(None)

    try:
        result.append(r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'])
    except Exception as e:
        print(e,"totalMarketCap")
        result.append(None)

    try:
        result.append(tag)
    except Exception as e:
        print(e,"tag")
        result.append(None)

    try:
        result.append(token_dict[StockSymbol])
    except Exception as e:
        print(e,"token")
        result.append(None)

    try:
        result.append(sect_ind_dict[StockSymbol][0])
    except Exception as e:
        print(e,"sector")
        result.append(None)

    try:
        result.append(sect_ind_dict[StockSymbol][1] )
    except Exception as e:
        print(e,"idustru")
        result.append(None)

    try:
        result.append(date[0])
    except Exception as e:
        print(e,"date")
        result.append(None)

    try:
        result.append(date[1])
    except Exception as e:
        print(e,"series")
        result.append(None)
    Stoc_le.append(StockSymbol)
    return result


def get_metadata():
    data_list = []
    count = 0
    s = None
    for StockSymbol, date_series in sym_date_dict.items():
        if count % 200 == 0:
            s = requests.session()

            s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"})
            count = count + 1
            try:
                data_list.append(make_dataframe(s, StockSymbol, date_series))
            except Exception as e:
                print(e,"exceptio")

        else:

            count = count + 1
            try:
                data_list.append(make_dataframe(s, StockSymbol, date_series))
            except Exception as e:
                print(str(e),"exceptio")

    dp_meta_df_all = pd.DataFrame(data_list, columns=["Stock",
                                                      "companyName",
                                                      'pdSectorInd',
                                                      'totalMarketCap',
                                                      'companySize',
                                                      'token',
                                                      "Sector",
                                                      "Industry",
                                                      "Series",
                                                      "Date"
                                                      ]
                                  )
    if os.path.exists(REPORT):
        os.remove(REPORT)
        print("File deleted")
        dp_meta_df_all.to_csv(REPORT, index=False)
    else:
        dp_meta_df_all.to_csv(REPORT, index=False)


def trial(size):
    try:
        s = requests.session()

        s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"})

        for _ in range(size):
            sym = random.choice(list(sym_date_dict.keys()))
            make_dataframe(s, sym, sym_date_dict[sym])
        return True
    except Exception as e:
        print(e)
        return False

if __name__ == '__main__':
    # Getting path of Project Directory
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Getting path of holiday csv file
    HOLIDAY_FILE_PATH = os.path.join(BASE_DIR, 'master/holiday.csv')

    # Downloading bhav file and returning its absolute path
    BHAV_DATA,date_str = get_Bhav_file(HOLIDAY_FILE_PATH, BASE_DIR)

    # Getting path of Token security csv file
    Token_security = os.path.join(BASE_DIR, 'master/Token_security.csv')

    # Getting path of NSE DELIVERY file
    NSE_DELIVERY = os.path.join(BASE_DIR, 'master/NSE DELIVERY.xlsx')

    # Generating path of final report
    REPORT = os.path.join(BASE_DIR, 'report/StockMetadata_{}.csv'.format(date_str))

    # Getting SYMBOL and DATE  from bhav data file
    live_data_sym_list = pd.read_csv(BHAV_DATA, usecols=['SYMBOL', ' DATE1',' SERIES']).values
    sym_date_dict = {SYMBOL: [DATE,SERIES] for SYMBOL, DATE, SERIES in live_data_sym_list}
    sym_date_dict = {k: v for k, v in sym_date_dict.items() if v[0] in [' BE', ' EQ', ' SM']}
    # Getting SYMBOL and DATE  from bhav data file
    data_list = pd.read_csv(Token_security, usecols=['token', 'Symbol']).values
    token_dict = {key: value for value, key in data_list}

    # Getting SYMBOL,Sector and Industry  from NSE DELIVERY file
    sect_ind_list = pd.read_excel(NSE_DELIVERY, usecols=['StockSymbol', 'CD_Sector', 'CD_Industry1']).values
    sect_ind_dict = {key: [value1, value2] for key, value1, value2 in sect_ind_list}
    if trial(2):
        print("Trial Is Successfull")
        get_metadata()
    else:
        print("Trial Is UnSuccessfull")
