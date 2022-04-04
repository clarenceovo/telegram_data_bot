import logging
import json
import io
import os
import pandas as pd
import seaborn as sns
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from api_data_service.api import data_service
from  IGDataSnapshotter.IGDataSnapshotter import IGDataSnapshotter
from datetime import datetime , date,timedelta
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')
matplotlib.style.use('ggplot')
import requests
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class financial_data_bot:
    def __init__(self):
        self.__config = json.load(open(os.path.join(os.getcwd(),'config/config.json')))
        self.__api = "http://"+self.__config['api_endpoint']
        self.__data_service = data_service()
        self.__image_buffer = io.BytesIO()
        self.__ig_credential = self.__config['ig_credential']
        self.__ticker=json.load(open(os.path.join(os.getcwd(),"config/ticker.json")))
        if self.__config is not None:
            logger.info("Loaded configuration successfully")
        self.updater = Updater(self.__config['telegram_token'])
        #self.__ig_conn = IGDataSnapshotter(self.__ig_credential['identifier'], self.__ig_credential['password'],self.__ig_credential['api_key'])
        #Disable IG Quote
        self.__ig_quote = {}
        self.__ig_quote_ts = 0



    def _get_yield_curve_chart(self, update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        buffer = io.BytesIO()
        custom_dict = {"US3M":0,"US6M":1,"US9M":2,'US1Y': 3, 'US2Y': 4, 'US5Y': 5,'US10Y':6,"US20Y":7,"US30Y":8}
        res = self.__data_service.get_yield()
        data_dict = {}
        for symbol ,item in res:
            data_dict[symbol]=[item.update_time,item.open,item.high,item.low,item.last,item.prev_close]
        df = pd.DataFrame.from_dict(data_dict,orient='index')
        df.columns = ['UPDATE_TIME','OPEN','HIGH','LOW','LAST','PREV_CLOSE']
        df['sort']=df.index
        df['UPDATE_TIME'] = pd.to_datetime(df['UPDATE_TIME'],format='%Y-%m-%dT%H:%M:%S.%f%z')
        df = df.sort_index(key=lambda x:x.map(custom_dict))
        tdy = datetime.now()
        time_range= [tdy+timedelta(days=30*3),tdy+timedelta(days=30*6),tdy+timedelta(days=365),tdy+timedelta(days=365*2),
                     tdy+timedelta(days=365*5),tdy+timedelta(days=365*10),tdy+timedelta(days=365*20),tdy+timedelta(days=365*30)]
        #df['UPDATE_TIME']=df.apply(lambda x:x.strftime('%Y/%m/%d %H:%M:%S'),axis=1)
        ret = df[['LAST']]
        fig, ax = plt.subplots()
        ax.plot(time_range, ret['LAST'], color="blue")
        plt.suptitle(f"Yield Curve", size=12)
        plt.savefig(buffer, format='jpeg')
        msg = ret.to_string(index=True,header=False)
        ret=ret.to_dict()['LAST']
        yield_spread = float(ret['US10Y'].strip('%')) - float(ret['US2Y'].strip('%'))
        msg+=\
f"""\n___________________
2Y-10Y Spread: {round(yield_spread,4)}"""
        update.message.reply_photo(photo=buffer.getvalue(), caption=msg)
        #update.message.reply_text(ret.to_string(index=True))

        return
    def _help(self, update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        msg = """Command :
/hkshortvol <ticker> <session> 
(eg: /hkshortvol 2800 AM) 
AM: Morning Session PM:Whole Trading Day
It returns a chart with 21 days shortselling data

/cryptooi <ticker> 
(eg: /cryptooi SOL-PERP)
It returns a chart with 21 days open interest data of the ticker

/hsioi 
It returns 30 Days HSI future open interest data 

/hkstockoi <ticker> <mode>
It returns the latest stock option OI change of a stock
<mode> :type 'c2' to next the next forward option 

/igmarket (Currently Disable :( )
Get the live IG Market Price
        """
        update.message.reply_text(msg)
    def __get_contract_month(self):
        current = datetime.now()
        forward = datetime.now() + timedelta(days=28)
        forward = forward.strftime("%Y-%m-01")
        current = current.strftime("%Y-%m-01")
        return (current,forward)

    def __on_trigger(self,update: Update):
        chat_history = update.message.chat
        id = chat_history.id
        user_name = chat_history.username
        if user_name is None:
            user_name= chat_history.title
        time= datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        logger.info(f"User:{user_name};ID:{id};Time:{time}")

    def __get_chart_limit(self,df_col):
        max = df_col.max()
        max = max*1.1
        return max if max % 100 == 0 else max + 100 - max % 100

    def _get_stock_option_oi(self,update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        buffer = io.BytesIO()
        cmd = update.message.text.split("/hkstockoi")[1].split(' ')
        cmd.remove('')
        if len(cmd)==0:
            update.message.reply_text("Wrong Command Parameter. Please input the ticker")
            return
        ticker = cmd[0]
        if len(cmd)>1:
            mode = cmd[1] #C2
        else:
            mode = 'c1'
        if ticker is not None:
            if mode == 'c2':
                month = self.__get_contract_month()[1]
            else:
                month = self.__get_contract_month()[0]

            end = datetime.utcnow()+timedelta(hours=8)
            end_str = end.strftime("%Y-%m-%d")
            start_str =  end - timedelta(days=7)
            start_str=start_str.strftime("%Y-%m-%d")
            ret = self.__data_service.get_stock_option_oi_by_ticker(ticker=ticker,month=month,start=start_str,end=end_str)
            last_record_date = ret['date'].max() #last record date
            ret = ret.query(f'date =="{last_record_date}"')
            chart_df = ret.copy()
            call_df = chart_df.query("type == 'C'")
            put_df = chart_df.query("type == 'P'")
            fig = plt.figure()
            limit = self.__get_chart_limit(chart_df['open_interest'])
            axe2 = plt.subplot(122)
            axe2.barh(put_df['strike'], put_df['open_interest'], align='center', color='red')
            axe2.set_title("PUT")
            axe2.set_xlim(0, limit)
            axe1 = plt.subplot(121, sharey=axe2)
            axe1.set_xlim(0,limit)
            axe1.barh(call_df['strike'], call_df['open_interest'], align='center', color='green')
            axe1.set_title("CALL")
            axe1.invert_xaxis()
            plt.ylabel("Strike")
            last_record_date = last_record_date[:10]
            plt.suptitle(f"Open Interest@{last_record_date} Option Month:{month[:7]}", size=12)
            plt.savefig(buffer, format='jpeg')

            #ret['price_delta'] = ret.apply(lambda x: int(x['strike']) - price_settle, axis=1)
            ret['oi_delta_abs'] = ret.apply(lambda x: abs(int(x['oi_change'])), axis=1)
            call_df = ret.query("type == 'C'")
            call_df = call_df.sort_values(by=["oi_delta_abs","open_interest"],ascending=False)
            put_df = ret.query("type == 'P'")
            put_df = put_df.sort_values(by=["oi_delta_abs","open_interest"],ascending=False)
            call_ret = call_df[['strike','close',"open_interest",'implied_vol','oi_change']][:7]
            put_ret = put_df[['strike', 'close', "open_interest", 'implied_vol', 'oi_change']][:7]
            call_ret.columns = ['strike','close',"OI",'IV','oi_delta']
            put_ret.columns = ['strike','close',"OI",'IV','oi_delta']


            ret=f"""
Ticker:{ticker} Option OI Change @{last_record_date}
CALL OI 
-----------
{call_ret.to_string(index=False,header=True,col_space=8)}

PUT  OI 
-----------
{put_ret.to_string(index=False,header=True,col_space=8)}
"""
            update.message.reply_photo(photo=buffer.getvalue(), caption=ret)




    def _get_crypto_open_interest(self,update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        cmd = update.message.text.split("/cryptooi")[1].split(' ')
        if len(cmd)==0:
            return
        cmd.remove('')
        if len(cmd)==0:
            return
        ticker = cmd[0]
        end = datetime.now() + timedelta(hours=8)
        end = end.strftime('%Y-%m-%d')
        start =datetime.now() + timedelta(hours=8)-timedelta(days=14)
        start =start.strftime('%Y-%m-%d')
        if ticker is not None:
            ret = requests.get(self.__api + "/crypto/customTimeRangeOpenInterest", params={
                "ticker": ticker,
                "exchange": 'FTX',
                "start":start,
                "end":end
            })
            if ret.status_code == 200:
                if len(ret.json()['data'])==0:
                    update.message.reply_text("Wrong Ticker Parameter")
                    return
                data = pd.DataFrame(ret.json()['data'])
                data=data[['datetime','price','open_interest']]
                data['datetime'] = pd.to_datetime(data['datetime'])
                data=data.drop_duplicates(subset='datetime', keep="last")
                data = data.set_index('datetime')
                f, axs = plt.subplots(2, 1,
                                      figsize=(18,10),
                                      sharey=False)
                open_interest_plt = sns.lineplot(data=data, x=data.index, y="open_interest",ax=axs[0])

                price_plt =sns.lineplot(data=data, x=data.index, y="price",ax=axs[1])
                buffer = io.BytesIO()
                plt.title(f'{ticker} OI@{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                plt.savefig(buffer,format='jpeg')
                update.message.reply_photo(photo=buffer.getvalue())
                buffer.close()

    def _get_hsi_future_open_interest(self,update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        buffer = io.BytesIO()
        ret = requests.get(self.__api + "/equity/HK/getHSIFutureOI")
        if ret.status_code == 200:
            data = pd.DataFrame(ret.json()['data'])
            data['date'] = pd.to_datetime(data['date']).dt.date
            data = data[data['date'] > (date.today() - timedelta(days=60))]
            fig, ax = plt.subplots()
            ax.plot(data['date'],data['current_price'], color="blue")
            ax.set_xlabel("Date", fontsize=12)
            ax.set_ylabel("Future Settlement Price", fontsize=12)
            ax2 = ax.twinx()
            ax2.plot(data['date'],data['open_interest'],'--',color="red")
            ax2.set_ylabel("Open Interest", fontsize=12)
            plt.title("HSI Future Open Interest (Last 60 Days)")
            plt.gcf().autofmt_xdate()

            plt.savefig(buffer, format='jpeg')
            ##For analytic caption
            data['price_change'] = data['current_price'] - data['current_price'].shift(1)
            data['oi_change']= data['open_interest'] - data['open_interest'].shift(1)
            ret = data[['date','current_price','price_change','oi_change']]
            ret = ret[ret['date'] > (date.today() - timedelta(days=21))]
            #print(ret)
            ret_str = f'Last 21 Days HSI Future OI Change\n' \
                      f'      Date       Price    Change  OI Change\n' \
                      f'{ret.to_string(index=False,header=False)}'

            update.message.reply_photo(photo=buffer.getvalue(),caption=ret_str)
            #update.message.reply_text(ret.to_string(index=False))


    def _get_HK_open_interest(self,update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        buffer = io.BytesIO()
        """

        :param stock_code:
        :param session:

        """
        cmd = update.message.text.split("/hkshortvol")[1].split(' ')
        cmd.remove('')
        if len(cmd)>0:
            if len(cmd)==1:
                ticker = int(cmd[0]) if cmd[0].isdigit() else None
                session = 'PM'
            elif len(cmd)==2:
                ticker = int(cmd[0]) if cmd[0].isdigit() else None
                session = cmd[1] if cmd[1]  in ['AM','PM'] else None

        else:
            ticker=2800
            session = 'PM'

        if ticker is not None and session is not None:
            ret = requests.get(self.__api+"/equity/HK/getShortSellingByTicker",params={
                "ticker":ticker,
                "session":session
            })
            if ret.status_code == 200:
                data = pd.DataFrame(ret.json()['data'])
                data['date'] = pd.to_datetime(data['date']).dt.date
                data['datetime']=pd.to_datetime(data['date'])
                #Past 14 Days data
                data = data[data['date']>(date.today()-timedelta(days=60))]
                data['date'] = data.apply(lambda x:x['date'].strftime("%m/%d"),axis=1)
                data = data.set_index("datetime")
                fig, ax = plt.subplots()
                ax.plot(data.index,data['shares'])
                ax.set_xlabel("Date", fontsize=12)
                ax.set_ylabel("Number of Shares", fontsize=12)
                #sns_plot = sns.lineplot(data=data , x=data.index,y="turnover",ax=axs[0])
                plt.gcf().autofmt_xdate()
                plt.savefig(buffer,format='jpeg')
                ret_df = data.tail(14)
                ret_df['turnover']= ret_df.apply(lambda x:"$"+f"{x['turnover']:,}",axis=1)
                ret_df['shares']= ret_df.apply(lambda x:' '+f"{x['shares']:,}"+' ',axis=1)
                ret_df.columns=[' Date  ',"Shares ",'Turnover($HKD)']
                ret_text = ret_df.to_string(index=False)
                #update.message.reply_text(ret_text)
                update.message.reply_photo(photo=buffer.getvalue(),caption=ret_text)
                buffer.close()
        else:
            update.message.reply_text("Wrong Command Parameter")


    def _ig_market(self,update: Update, context: CallbackContext):
        self.__on_trigger(update)
        self.__get_snapshot()
        ret = self.__get_ig_quote_string()
        update.message.reply_text(ret)





    def __get_ig_quote_string(self):
        ret = f"Updated Time:{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}\n" \
              f"--------------------------------------------------------------\n"
        if self.__ig_quote is not None and isinstance(self.__ig_quote,dict):
            for ticker in self.__ig_quote.keys():
                ret+= f'{ticker}: {self.__ig_quote[ticker]["bid"]}/{self.__ig_quote[ticker]["ask"]}\n'
        else:
            ret = 'Quote is not available'

        return ret
    def __get_bo_dict(self,bid,ask):
        return {
            "bid":bid,
            "ask":ask
        }

    def __get_snapshot(self):
        #trigger the update 10 second each
        if (datetime.now().timestamp()-self.__ig_quote_ts) >self.__config['ig_update_interval']:
            for ticker in self.__ticker.keys():
                ret = self.__ig_conn.get_market(self.__ticker[ticker])
                if 'snapshot' in ret.keys():
                    ret = ret['snapshot']
                    self.__ig_quote[ticker]= self.__get_bo_dict(ret['bid'],ret['offer'])
                else:
                    logger.error(f"Instrument Error:{ticker}")
                    logger.error(ret)
            self.__ig_quote_ts =datetime.now().timestamp()


    def __serive_unavailable(self,update: Update, context: CallbackContext) -> None:
        update.message.reply_text("Sorry. This service is not available at the moment.\n"
                                  "It will be back soon :)")



    def run(self):
        logger.info(f"Bot starts at:{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
        self.dispatcher = self.updater.dispatcher
        self.dispatcher.add_handler(CommandHandler("yield", self._get_yield_curve_chart))
        self.dispatcher.add_handler(CommandHandler("hkstockoi", self._get_stock_option_oi))
        self.dispatcher.add_handler(CommandHandler("hkshortvol", self._get_HK_open_interest))
        self.dispatcher.add_handler(CommandHandler("cryptooi", self._get_crypto_open_interest))
        self.dispatcher.add_handler(CommandHandler("hsioi", self._get_hsi_future_open_interest))
        #self.dispatcher.add_handler(CommandHandler("igmarket", self._ig_market))
        self.dispatcher.add_handler(CommandHandler("igmarket", self.__serive_unavailable))
        self.dispatcher.add_handler(CommandHandler("help", self._help))
        self.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, self._general_query))
        #logger.info(f"Bot starts at:{datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
        self.updater.start_polling()

    def _general_query(self,update: Update, context: CallbackContext) -> None:
        msg = update.message.text
        if "萬里長城長又長" in msg:
            self.__on_trigger(update)
            update.message.reply_text("我的尾水比他長🙏🏻")

if __name__ == '__main__':
    bot = financial_data_bot()
    bot.run()