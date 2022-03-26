import requests
import json
import os
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
class data_service:
    def __init__(self):
        self.__config = json.load(open(os.path.join(os.getcwd(),'config/config.json')))
        self.__api = "http://"+self.__config['api_endpoint']


    def get_stock_option_oi_by_ticker(self,ticker,month,start,end)->pd.DataFrame:
        res = requests.get(self.__api + "/equity/HK/getHSIStockOptionOI", params={
            "ticker": ticker,
            "month": month,
            "start": start,
            "end": end,
        })
        if res.status_code == 200:
            return pd.DataFrame(res.json()['data'])

        else:
            return None


