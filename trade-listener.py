from dotenv import load_dotenv
import os
from pybit.unified_trading import WebSocket
import psycopg
import datetime
import logging

load_dotenv()

logging.basicConfig(filename='pybit.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST=os.getenv('POSTGRES_HOST')
POSTGRES_PORT=os.getenv('POSTGRES_PORT')
POSTGRES_USER=os.getenv('POSTGRES_USER')
POSTGRES_DB=os.getenv('POSTGRES_DB')



def main():

    connection = psycopg.connect(
        host = POSTGRES_HOST,
        port = POSTGRES_PORT,
        user = POSTGRES_USER,
        dbname = POSTGRES_DB,
        password = POSTGRES_PASSWORD
    )

    cursor = connection.cursor()


    ws_linear_1 = WebSocket(testnet=False, channel_type='linear')
    ws_private = WebSocket(testnet = False, demo = True, channel_type = "private", api_key = API_KEY, api_secret = API_SECRET)
    


    def handle_message(msg, cursor = cursor):

        query = 'INSERT INTO raw_trade_data (TIME, SYMBOL, PRICE, QUANTITY)' +\
                ' VALUES (%s, %s, %s, %s)'
        
        timestamp = datetime.datetime.fromtimestamp(msg['ts'] / 1000)

        record_to_insert = (timestamp, msg['topic'], msg['data'][0]['p'], msg['data'][0]['v'])
        cursor.execute(query, record_to_insert)
        connection.commit()

    def handle_wallet(msg, cursor = cursor):
        query = 'INSERT INTO raw_wallet_data (TIME, TOTAL_EQUITY)' +\
                ' VALUES (%s, %s)'
        timestamp = datetime.datetime.fromtimestamp(msg['creationTime'] / 1000)
        record_to_insert = (timestamp, msg['data'][0]['totalEquity'])
        cursor.execute(query, record_to_insert)
        connection.commit()

    ws_linear_1.trade_stream(symbol = ['DOGEUSDT','WIFUSDT','1000PEPEUSDT','TRUMPUSDT','FARTCOINUSDT', 'PNUTUSDT', 'POPCATUSDT'], callback = handle_message)
    ws_private.wallet_stream(callback = handle_wallet)


    while True:
        pass

if __name__ == '__main__':
    main()
    
    
    

