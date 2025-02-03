from pybit.unified_trading import WebSocket
import logging
from time import sleep

logging.basicConfig(filename='pybit.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

ws_linear = WebSocket(testnet = False,
               channel_type='linear')

def handle_linear_orderbook(message):

    topic = "orderbook.50.TRUMPUSDT"
    ws_linear._process_delta_orderbook(message, topic)
    print(ws_linear.data[topic])


ws_linear.orderbook_stream(50, 'TRUMPUSDT', handle_linear_orderbook)


while True:
    sleep(1)
