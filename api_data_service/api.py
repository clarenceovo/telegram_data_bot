import requests
import json
import os
import pandas as pd
import logging
import concurrent.futures

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime



class Ticker:
    def __init__(self,source,ticker,open,high,low,close,latest,prev_close,update_time):
        self.source=source
        self.product_name = ticker
        self.open=open
        self.high=high
        self.low=low
        self.close=close
        self.last=latest
        self.prev_close = prev_close
        self.update_time = update_time
        self.create_time = datetime.utcnow()


class data_service:
    def __init__(self):
        self.__config = json.load(open(os.path.join(os.getcwd(),'config/config.json')))
        self.__api = "http://"+self.__config['api_endpoint']
        self.__cnbc_api = "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
        Executor: ThreadPoolExecutor = None
        self.data_service_executor = ThreadPoolExecutor(500)

    def get_stock_option_oi_by_ticker(self,ticker,month,start,end)->pd.DataFrame:
        res = requests.get(self.__api + "/equity/HK/getHSIStockOptionOI", params={
            "ticker": ticker,
            "month": month,
            "start": start,
            "end": end,
        })
        if res.status_code == 200:
            return pd.DataFrame(res.json()['data'])

    def get_index_future_oi(self,month,start,end)->pd.DataFrame:
        res = requests.get(self.__api + "/equity/HK/getHSIIndexOptionOI", params={
            "month": month,
            "start": start,
            "end": end,
        })
        if res.status_code == 200:
            return pd.DataFrame(res.json()['data'])


    def get_cnbc_quote(self,ticker:str):
        param = {
            "symbols":None,
            "requestMethod": "itv",
            "noform": 1,
            "partnerId": 2,
            "fund": 1,
            "exthrs": 1,
            "output": "json",
            "events": 1
        }
        param['symbols']= ticker
        res = requests.get(self.__cnbc_api,param)
        if res.status_code ==200:
            return (ticker,res.json())
        else:
            logger.error(res.content)
            raise Exception
            logger.error("Failed to call CNBC ticker")

    def _process_ticker(self,symbol,json_dict):
        ticker = json_dict['FormattedQuoteResult']['FormattedQuote'][0]
        try:
            return (symbol,Ticker("CNBC",
                          ticker=ticker['symbol'],
                          open=ticker['open'],
                          high=ticker['high'],
                          low=ticker['low'],
                          close=None,
                          latest=ticker['last'],
                          prev_close=ticker['previous_day_closing'],
                          update_time=ticker['last_time']))
        except Exception as e:
            logger.error(f'ERROR:{e}')
            return None

    def get_yield(self):
        result = []
        job_list = []
        ret = []
        requests_list = ["US3M","US6M","US1Y","US2Y","US5Y","US10Y","US20Y","US30Y"]
        for item in requests_list:
            job = self.data_service_executor.submit(self.get_cnbc_quote,item)
            job_list.append(job)
        for future in concurrent.futures.as_completed(job_list):
            result.append(future.result())
        for ticker,item in result:
            formatted = self._process_ticker(ticker,item)
            ret.append(formatted)
        return ret

    def get_fx(self):
        result = []
        job_list = []
        ret = []
        requests_list = ["EUR=", "JPY=","AUD=", "GBP=", "CAD=", "CHF=", "SGD=", "CNY=", "HKD="]
        for item in requests_list:
            job = self.data_service_executor.submit(self.get_cnbc_quote, item)
            job_list.append(job)
        for future in concurrent.futures.as_completed(job_list):
            result.append(future.result())
        for ticker, item in result:
            formatted = self._process_ticker(ticker, item)
            ret.append(formatted)
        return ret




