from dotenv import load_dotenv
import os
import logging
import psycopg
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
import itertools
import json
import time
import tempfile
import shutil

# Set up logging
logging.basicConfig(filename='cointegration.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

# Load environment variables
load_dotenv()

POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_DB = os.getenv('POSTGRES_DB')
COINTEGRATION_REFRESH_SECONDS = os.getenv('COINTEGRATION_REFRESH_SECONDS')

def main():
    try:
        # Log start time of analysis
        logger.info(f"Starting cointegration analysis at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Establish database connection
        connection = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            dbname=POSTGRES_DB,
            password=POSTGRES_PASSWORD
        )

        # Load dataframes with 360 candles, sorted by date ascending (60-minute cointegration relationship)
        df_FARTCOINUSDT = pd.read_sql_query(
            "SELECT * FROM ohlc_data_10sec WHERE symbol = 'publicTrade.FARTCOINUSDT' ORDER BY date DESC LIMIT 360;",
            connection
        ).sort_values(by='date').reset_index(drop=True)
        
        df_WIFUSDT = pd.read_sql_query(
            "SELECT * FROM ohlc_data_10sec WHERE symbol = 'publicTrade.WIFUSDT' ORDER BY date DESC LIMIT 360;",
            connection
        ).sort_values(by='date').reset_index(drop=True)
        
        df_DOGEUSDT = pd.read_sql_query(
            "SELECT * FROM ohlc_data_10sec WHERE symbol = 'publicTrade.DOGEUSDT' ORDER BY date DESC LIMIT 360;",
            connection
        ).sort_values(by='date').reset_index(drop=True)
        
        df_PNUTUSDT = pd.read_sql_query(
            "SELECT * FROM ohlc_data_10sec WHERE symbol = 'publicTrade.PNUTUSDT' ORDER BY date DESC LIMIT 360;",
            connection
        ).sort_values(by='date').reset_index(drop=True)
        
        df_POPCATUSDT = pd.read_sql_query(
            "SELECT * FROM ohlc_data_10sec WHERE symbol = 'publicTrade.POPCATUSDT' ORDER BY date DESC LIMIT 360;",
            connection
        ).sort_values(by='date').reset_index(drop=True)
        
        # List of coins
        coins = ['FARTCOINUSDT', 'WIFUSDT', 'DOGEUSDT', 'PNUTUSDT', 'POPCATUSDT']

        # Dictionary of log closing prices
        closes = {
            'FARTCOINUSDT': np.log(df_FARTCOINUSDT['close']),
            'WIFUSDT': np.log(df_WIFUSDT['close']),
            'DOGEUSDT': np.log(df_DOGEUSDT['close']),
            'PNUTUSDT': np.log(df_PNUTUSDT['close']),
            'POPCATUSDT': np.log(df_POPCATUSDT['close'])
        }

        # Generate all possible pairs with consistent alphabetical order (coin1 < coin2)
        pairs = [pair for pair in itertools.combinations(coins, 2) if pair[0] < pair[1]]

        # List to store results
        results = []

        # Test each pair for cointegration in one direction only
        for pair in pairs:
            coin1, coin2 = pair
            y = closes[coin1]  # coin1 is always the first alphabetically (y)
            x = closes[coin2]  # coin2 is always the second alphabetically (x)
            test_stat, p_value, crit_values = coint(y, x)

            # Check if cointegration is significant at 10% level
            if p_value < 0.1:
                # Determine significance level
                if test_stat < crit_values[0]:
                    significance = '1%'
                elif test_stat < crit_values[1]:
                    significance = '5%'
                elif test_stat < crit_values[2]:
                    significance = '10%'

                # Compute hedge ratio using OLS
                model = sm.OLS(y, sm.add_constant(x)).fit()
                hedge_ratio = float(model.params[1])

                # Calculate spread and its statistics
                spread = y - hedge_ratio * x
                mean_spread = float(spread.mean())
                std_spread = float(spread.std())

                # Store result if cointegration is significant
                results.append({
                    'y': coin1,
                    'x': coin2,
                    'hedge_ratio': hedge_ratio,
                    'mean_spread': mean_spread,
                    'std_spread': std_spread
                })

                # Log individual cointegrated pair result
                logger.info(f"Found cointegrated pair: y = {coin1}, x = {coin2}, "
                            f"Significance level: {significance}, "
                            f"Hedge Ratio: {hedge_ratio:.4f}, Mean Spread: {mean_spread:.4f}, "
                            f"Std Spread: {std_spread:.4f}")

        # Print summary of results
        if results:
            print(f"Total cointegrated pairs found: {len(results)}")
        else:
            print("No cointegrated pairs found at 10 significance level or better.")

        # Write results to JSON file atomically
        with tempfile.NamedTemporaryFile('w', delete=False) as tmp:
            json.dump(results, tmp, indent=4)
            tmp.flush()
        shutil.move(tmp.name, 'cointegration_results.json')
        
        # Print confirmation of JSON update
        print(f"Updated cointegration_results.json at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        # Close connection
        connection.close()

    except Exception as e:
        logger.error(f"Error in cointegration analysis: {e}")
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    while True:
        main()
        logger.info(f"Sleeping for {COINTEGRATION_REFRESH_SECONDS} seconds before next run...")
        time.sleep(COINTEGRATION_REFRESH_SECONDS)  # Run every 5 minutes, rolling window 360 data points (60 minutes)