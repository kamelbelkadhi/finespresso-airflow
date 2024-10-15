import pandas as pd
import yfinance as yf
import logging
from datetime import datetime
import numpy as np
from pandas_market_calendars import get_calendar
from utils.price_move_db_util import store_price_move, PriceMove

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

index_symbol = 'SPY'
EXCHANGE = 'NASDAQ'

def get_price_data(ticker, published_date):
    logger.info(f"Getting price data for {ticker} on {published_date}")
    row = pd.Series({'ticker': ticker, 'published_date': published_date, 'market': 'market_open'})  # Assume market_open, adjust if needed
    processed_row = set_prices(row)
    
    if processed_row['begin_price'] is None or processed_row['end_price'] is None:
        logger.warning(f"Unable to get price data for {ticker} on {published_date}")
        return None

def set_prices(row):
    symbol = row['ticker']
    logger.info(f"Processing price data for {symbol}")

    if isinstance(row['published_date'], pd.Timestamp):
        today_date = row['published_date'].to_pydatetime()
    else:
        today_date = datetime.strptime(row['published_date'], '%Y-%m-%d %H:%M:%S%z')
    logger.info(f"Today's date for {symbol}: {today_date}")

    # Get the exchange calendar
    exchange_calendar = get_calendar(EXCHANGE)

    # Find the previous and next trading days
    # Get valid trading days up to today
    valid_trading_days = exchange_calendar.valid_days(start_date=today_date - pd.Timedelta(days=30), end_date=today_date + pd.Timedelta(days=30))
    # Find the previous trading day
    previous_trading_day = valid_trading_days[valid_trading_days < today_date][-1].date()
    # Find the next trading day
    next_trading_day = valid_trading_days[valid_trading_days > today_date][0].date()
    logger.info(f"Adjusted dates for {symbol}: Previous: {previous_trading_day}, Next: {next_trading_day}")
    
    yf_previous_date = previous_trading_day.strftime('%Y-%m-%d')
    yf_today_date = today_date.strftime('%Y-%m-%d')
    yf_next_date = next_trading_day.strftime('%Y-%m-%d')

    try:
        logger.info(f"Fetching stock data for {symbol}")
        data = yf.download(symbol, yf_previous_date, yf_next_date)
        logger.info(f"Fetching index data for {index_symbol}")
        index_data = yf.download(index_symbol, yf_previous_date, yf_next_date)
        
        if data.empty or index_data.empty:
            logger.warning(f"No data available for {symbol} or index in the specified date range")
            raise ValueError("No data available for the specified date range")

        logger.info(f"Determining prices for {symbol} based on market: {row['market']}")
        if row['market'] == 'market_open':
            row['begin_price'] = data.loc[yf_today_date]['Open']
            row['end_price'] = data.loc[yf_today_date]['Close']
            row['index_begin_price'] = index_data.loc[yf_today_date]['Open']
            row['index_end_price'] = index_data.loc[yf_today_date]['Close']
        elif row['market'] == 'pre_market':
            row['begin_price'] = data.loc[yf_previous_date]['Close']
            row['end_price'] = data.loc[yf_today_date]['Open']
            row['index_begin_price'] = index_data.loc[yf_previous_date]['Close']
            row['index_end_price'] = index_data.loc[yf_today_date]['Open']
        elif row['market'] == 'after_market':
            row['begin_price'] = data.loc[yf_today_date]['Close']
            row['end_price'] = data.loc[yf_next_date]['Open']
            row['index_begin_price'] = index_data.loc[yf_today_date]['Close']
            row['index_end_price'] = index_data.loc[yf_next_date]['Open']
        
        row['price_change'] = row['end_price'] - row['begin_price']
        row['index_price_change'] = row['index_end_price'] - row['index_begin_price']
        row['price_change_percentage'] = row['price_change'] / row['begin_price']
        row['index_price_change_percentage'] = row['index_price_change'] / row['index_begin_price']
        logger.info(f"Successfully set prices for {symbol}")
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        row['begin_price'] = None
        row['end_price'] = None
        row['index_begin_price'] = None
        row['index_end_price'] = None
        row['High'] = None
        row['Low'] = None
        row['Volume'] = None
    return row

def create_price_moves(news_df):
    logger.info(f"Starting to create price moves for {len(news_df)} news items")
    news_df = news_df.reset_index(drop=True)
    processed_rows = []

    for index, row in news_df.iterrows():
        try:
            logger.info(f"Processing row {index} for ticker {row['ticker']}")
            processed_row = set_prices(row)
            processed_rows.append(processed_row)
        except Exception as e:
            logger.error(f"Error processing row {index} for ticker {row['ticker']}: {e}")
            logger.exception("Detailed traceback:")
            continue

    processed_df = pd.DataFrame(processed_rows)
    logger.info(f"Processed {len(processed_df)} rows successfully")

    required_price_columns = ['begin_price', 'end_price', 'index_begin_price', 'index_end_price']
    missing_columns = [col for col in required_price_columns if col not in processed_df.columns]
    if missing_columns:
        logger.warning(f"Missing columns in the DataFrame: {missing_columns}")
        return processed_df

    original_len = len(processed_df)
    processed_df.dropna(subset=required_price_columns, inplace=True)
    logger.info(f"Removed {original_len - len(processed_df)} rows with NaN values")

    try:
        logger.info("Calculating alpha and setting actual side")
        processed_df['daily_alpha'] = processed_df['price_change_percentage'] - processed_df['index_price_change_percentage']
        processed_df['actual_side'] = np.where(processed_df['price_change_percentage'] >= 0, 'LONG', 'SHORT')
        logger.info("Successfully calculated alpha and set actual side")
    except Exception as e:
        logger.error(f"Error in calculations: {e}")

    logger.info(f"Finished creating price moves. Final DataFrame has {len(processed_df)} rows")
    
    # Store price moves in the database
    for _, row in processed_df.iterrows():
        try:
            price_move = create_price_move(
                news_id=row['news_id'],
                ticker=row['ticker'],
                published_date=row['published_date'],
                begin_price=row['begin_price'],
                end_price=row['end_price'],
                index_begin_price=row['index_begin_price'],
                index_end_price=row['index_end_price'],
                volume=row.get('Volume'),
                market=row['market'],
                price_change=row['price_change'],
                price_change_percentage=row['price_change_percentage'],
                index_price_change=row['index_price_change'],
                index_price_change_percentage=row['index_price_change_percentage'],
                actual_side=row['actual_side']
            )
            store_price_move(price_move)
        except Exception as e:
            logger.error(f"Error storing price move for news_id {row['news_id']}: {e}")

    return processed_df

def create_price_move(news_id, ticker, published_date, begin_price, end_price, index_begin_price, index_end_price, volume, market, price_change, price_change_percentage, index_price_change, index_price_change_percentage, actual_side, predicted_side=None):
    daily_alpha = price_change_percentage - index_price_change_percentage
    return PriceMove(
        news_id=news_id,
        ticker=ticker,
        published_date=published_date,
        begin_price=begin_price,
        end_price=end_price,
        index_begin_price=index_begin_price,
        index_end_price=index_end_price,
        volume=volume,
        market=market,
        price_change=price_change,
        price_change_percentage=price_change_percentage,
        index_price_change=index_price_change,
        index_price_change_percentage=index_price_change_percentage,
        daily_alpha=daily_alpha,
        actual_side=actual_side,
        predicted_side=predicted_side
    )