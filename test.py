                
from dotenv import load_dotenv
import os
import logging
from pybit.unified_trading import HTTP
import json
import time
import psycopg
import numpy as np

# Delay to ensure cointegration data is ready
time.sleep(5)

# Set up logging
logging.basicConfig(filename='trading.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()



# Load environment variables
load_dotenv()
api_key = os.getenv('BYBIT_API_KEY')
api_secret = os.getenv('BYBIT_API_SECRET')
ENTRY_THRESHOLD = float(os.getenv('ENTRY_THRESHOLD'))
RISK_PER_TRADE = float(os.getenv('RISK_PER_TRADE'))
MIN_ORDER_VALUE = float(os.getenv('MIN_ORDER_VALUE'))
TRADING_RUN_INTERVAL = float(os.getenv('TRADING_RUN_INTERVAL'))
TRADING_TAKER_FEE = float(os.getenv('TRADING_TAKER_FEE'))
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST=os.getenv('POSTGRES_HOST')
POSTGRES_PORT=os.getenv('POSTGRES_PORT')
POSTGRES_USER=os.getenv('POSTGRES_USER')
POSTGRES_DB=os.getenv('POSTGRES_DB')

# Initialize connection to DB
connection = psycopg.connect(
    host = POSTGRES_HOST,
    port = POSTGRES_PORT,
    user = POSTGRES_USER,
    dbname = POSTGRES_DB,
    password = POSTGRES_PASSWORD
)

cursor = connection.cursor()

# Initialize Bybit session
session = HTTP(testnet=False, demo=True, api_key=api_key, api_secret=api_secret)

# Track open positions: key is (y, x), value is position details
open_positions = {}

logger.info("Trading bot started...")

def main():
    while True:
        try:
            # Read cointegration results
            with open('cointegration_results.json', 'r') as f:
                cointegrated_pairs = json.load(f)

            # Step 1: Check all open positions first
            for pair_key in list(open_positions.keys()):
                y, x = pair_key
                position = open_positions[pair_key]

                # Fetch current prices
                price_y = float(session.get_tickers(category='linear', symbol=y)['result']['list'][0]['lastPrice'])
                price_x = float(session.get_tickers(category='linear', symbol=x)['result']['list'][0]['lastPrice'])

                # Calculate fees
                entry_fees = TRADING_TAKER_FEE * (position['price_y'] * position['quantity_y'] + position['price_x'] * position['quantity_x'])
                exit_fees = TRADING_TAKER_FEE * (price_y * position['quantity_y'] + price_x * position['quantity_x'])

                # Calculate profit from price movements
                if position['type'] == 'long_spread':
                    profit_from_movements = (price_y - position['price_y']) * position['quantity_y'] + (position['price_x'] - price_x) * position['quantity_x']
                elif position['type'] == 'short_spread':
                    profit_from_movements = (position['price_y'] - price_y) * position['quantity_y'] + (price_x - position['price_x']) * position['quantity_x']

                # Calculate total profit
                total_profit = profit_from_movements - entry_fees - exit_fees

                # Determine if we should exit
                should_exit = (total_profit >= 0.5) or (total_profit <= -0.3)

                # Log position details (fixing the undefined pnl_x and pnl_y)
                logger.info(f"Existing position: {position['type']}, Pair: {y}/{x}, Total Profit: {total_profit:.2f}, Should exit: {should_exit}")

                # Exit position if conditions are met
                if should_exit:
                    if position['type'] == 'long_spread':
                        resp_y = session.place_order(category='linear', symbol=y, side='Sell', orderType='Market', qty=str(position['quantity_y']))
                        resp_x = session.place_order(category='linear', symbol=x, side='Buy', orderType='Market', qty=str(position['quantity_x']))
                    else:  # short_spread
                        resp_y = session.place_order(category='linear', symbol=y, side='Buy', orderType='Market', qty=str(position['quantity_y']))
                        resp_x = session.place_order(category='linear', symbol=x, side='Sell', orderType='Market', qty=str(position['quantity_x']))

                    if resp_y['retCode'] == 0 and resp_x['retCode'] == 0:
                        logger.info(f"Exited {position['type']}: Pair {y}/{x}, Total Profit: {total_profit:.2f}")
                        del open_positions[pair_key]
                    else:
                        logger.error(f"Failed to exit {position['type']} {y}/{x}: {resp_y['retMsg']}, {resp_x['retMsg']}")

                        
            # Step 6: Process entry for cointegrated pairs not already in open positions
            for pair in cointegrated_pairs:
                y = pair['y']
                x = pair['x']
                hedge_ratio = pair['hedge_ratio']
                mean_spread = pair['mean_spread']
                std_spread = pair['std_spread']
                pair_key = (y, x)

                if pair_key not in open_positions:
                    # Fetch current prices
                    price_y = float(session.get_tickers(category='linear', symbol=y)['result']['list'][0]['lastPrice'])
                    price_x = float(session.get_tickers(category='linear', symbol=x)['result']['list'][0]['lastPrice'])


                    # Calculate current spread and z-score
                    current_spread = np.log(price_y) - hedge_ratio * np.log(price_x)
                    z = (current_spread - mean_spread) / std_spread

                    # Calculate position sizes
                    total_available_balance = float(session.get_wallet_balance(accountType='UNIFIED')['result']['list'][0]['totalAvailableBalance']) 
                    D = RISK_PER_TRADE * total_available_balance
                    quantity_y = int(D / price_y)
                    quantity_x = int(abs(quantity_y * hedge_ratio))

                    # Ensure order value meets minimum
                    if quantity_y * price_y < MIN_ORDER_VALUE or quantity_x * price_x < MIN_ORDER_VALUE:
                        logger.warning(f"Order value too small for {y}/{x}, skipping entry")
                        continue

                    if z > ENTRY_THRESHOLD:
                        # Short the spread: short y, long x
                        resp_y = session.place_order(category='linear', symbol=y, side='Sell', orderType='Market', qty=str(quantity_y))
                        resp_x = session.place_order(category='linear', symbol=x, side='Buy', orderType='Market', qty=str(quantity_x))
                        if resp_y['retCode'] == 0 and resp_x['retCode'] == 0:
                            open_positions[pair_key] = {
                                'type': 'short_spread',
                                'quantity_y': quantity_y,
                                'quantity_x': quantity_x,
                                'entry_time': time.time(),
                                'hedge_ratio': hedge_ratio,
                                'mean_spread': mean_spread,
                                'std_spread': std_spread,
                                'price_x' : price_x,
                                'price_y' : price_y
                            }
                            logger.info(f"Entered short spread: Balance: {total_available_balance}, Short {quantity_y} {y}, Long {quantity_x} {x}, Z-score: {z:.2f}")
                        else:
                            logger.error(f"Failed to enter short spread {y}/{x}: {resp_y['retMsg']}, {resp_x['retMsg']}")

                    elif z < -ENTRY_THRESHOLD:
                        # Long the spread: long y, short x
                        resp_y = session.place_order(category='linear', symbol=y, side='Buy', orderType='Market', qty=str(quantity_y))
                        resp_x = session.place_order(category='linear', symbol=x, side='Sell', orderType='Market', qty=str(quantity_x))
                        if resp_y['retCode'] == 0 and resp_x['retCode'] == 0:
                            open_positions[pair_key] = {
                                'type': 'long_spread',
                                'quantity_y': quantity_y,
                                'quantity_x': quantity_x,
                                'entry_time': time.time(),
                                'hedge_ratio': hedge_ratio,
                                'mean_spread': mean_spread,
                                'std_spread': std_spread,
                                'price_x' : price_x,
                                'price_y' : price_y
                            }
                            logger.info(f"Entered long spread: Balance: {total_available_balance}, Long {quantity_y} {y}, Short {quantity_x} {x}, Z-score: {z:.2f}")
                        else:
                            logger.error(f"Failed to enter long spread {y}/{x}: {resp_y['retMsg']}, {resp_x['retMsg']}")

            time.sleep(TRADING_RUN_INTERVAL)  # Wait 1 second to avoid constant fluctuations in Z-score

        except Exception as e:
            logger.error(f"Error in trading logic: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()




