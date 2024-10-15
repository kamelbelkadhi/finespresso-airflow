import logging
from datetime import datetime, time
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = 'postgresql://finespresso_user:7RkY1Sj27bzcoChbP7BlFLzOa9w77CxZ@dpg-cro7gpi3esus73bv7uvg-a.frankfurt-postgres.render.com/finespresso'#os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class PriceMove(Base):
    __tablename__ = 'price_moves'

    id = Column(Integer, primary_key=True)
    news_id = Column(String, nullable=False)
    ticker = Column(String, nullable=False)
    published_date = Column(DateTime, nullable=False)
    begin_price = Column(Float, nullable=False)
    end_price = Column(Float, nullable=False)
    index_begin_price = Column(Float, nullable=False)
    index_end_price = Column(Float, nullable=False)
    volume = Column(Integer)
    market = Column(String, nullable=False)
    price_change = Column(Float, nullable=False)
    price_change_percentage = Column(Float, nullable=False)
    index_price_change = Column(Float, nullable=False)
    index_price_change_percentage = Column(Float, nullable=False)
    daily_alpha = Column(Float, nullable=False)
    actual_side = Column(String(10), nullable=False)
    predicted_side = Column(String(10))
    predicted_move = Column(Float)

    def __init__(self, news_id, ticker, published_date, begin_price, end_price, index_begin_price, index_end_price,
                 volume, market, price_change, price_change_percentage, index_price_change, index_price_change_percentage,
                 daily_alpha, actual_side, predicted_side=None, predicted_move=None):
        self.news_id = news_id
        self.ticker = ticker
        self.published_date = published_date
        self.begin_price = begin_price
        self.end_price = end_price
        self.index_begin_price = index_begin_price
        self.index_end_price = index_end_price
        self.volume = volume
        self.market = market
        self.price_change = price_change
        self.price_change_percentage = price_change_percentage
        self.index_price_change = index_price_change
        self.index_price_change_percentage = index_price_change_percentage
        self.daily_alpha = daily_alpha
        self.actual_side = actual_side
        self.predicted_side = predicted_side
        self.predicted_move = predicted_move

def store_price_move(price_move):
    logger.info(f"Attempting to store price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")
    
    try:
        session = Session()
        session.add(price_move)
        logger.info(f"Price move added to session for news_id: {price_move.news_id}")
        session.commit()
        logger.info(f"Successfully committed price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")
    except Exception as e:
        logger.error(f"Error storing price move for news_id {price_move.news_id}: {str(e)}")
        logger.exception("Detailed traceback:")
        session.rollback()
    finally:
        session.close()

    # Verify that the price move was stored
    try:
        verify_session = Session()
        stored_price_move = verify_session.query(PriceMove).filter_by(news_id=price_move.news_id).first()
        if stored_price_move:
            logger.info(f"Verified: Price move for news_id {price_move.news_id} is in the database")
        else:
            logger.warning(f"Verification failed: Price move for news_id {price_move.news_id} not found in the database")
    except Exception as e:
        logger.error(f"Error verifying price move storage: {str(e)}")
    finally:
        verify_session.close()

# Create tables
Base.metadata.create_all(engine)