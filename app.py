import logging
import json
import io
import os
import pandas as pd
import seaborn as sns
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

/igmarket (Currently Disable :( )
Get the live IG Market Price
        """
        update.message.reply_text(msg)


    def __on_trigger(self,update: Update):
        chat_history = update.message.chat
        id = chat_history.id
        user_name = chat_history.username
        if user_name is None:
            user_name= chat_history.title
        time= datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        logger.info(f"User:{user_name};ID:{id};Time:{time}")


    def _get_crypto_open_interest(self,update: Update, context: CallbackContext) -> None:
        self.__on_trigger(update)
        cmd = update.message.text.split("/cryptooi")[1].split(' ')
        if len(cmd)==0:
            return
        cmd.remove('')
        if len(cmd)==0:
            return
        ticker = cmd[0]
        end = date.today().strftime('%Y-%m-%d')
        start =date.today()-timedelta(days=21)
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
            fig, ax = plt.subplots()
            ax.plot(data['date'],data['current_price'], color="blue")
            ax.set_xlabel("Date", fontsize=12)
            ax.set_ylabel("Future Settlement Price", fontsize=12)
            ax2 = ax.twinx()
            ax2.plot(data['date'],data['open_interest'],'--',color="red")
            ax2.set_ylabel("Open Interest", fontsize=12)
            plt.title("HSI Future Open Interest")
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
                data = data[data['date']>(date.today()-timedelta(days=21))]
                data['date'] = data.apply(lambda x:x['date'].strftime("%Y/%m/%d"),axis=1)
                data = data.set_index("datetime")
                fig, ax = plt.subplots()
                ax.plot(data.index,data['shares'])
                ax.set_xlabel("Date", fontsize=12)
                ax.set_ylabel("Number of Shares", fontsize=12)
                #sns_plot = sns.lineplot(data=data , x=data.index,y="turnover",ax=axs[0])
                plt.savefig(buffer,format='jpeg')
                data['turnover']= data.apply(lambda x:"$"+f"{x['turnover']:,}",axis=1)
                data['shares']= data.apply(lambda x:' '+f"{x['shares']:,}"+' ',axis=1)
                data.columns=['Date  ',"Shares ",'Turnover($HKD)']
                ret_text = data.to_string(index=False)
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