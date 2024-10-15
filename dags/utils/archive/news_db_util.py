import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func, and_, select, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import TIMESTAMP
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = 'postgresql://finespresso_user:7RkY1Sj27bzcoChbP7BlFLzOa9w77CxZ@dpg-cro7gpi3esus73bv7uvg-a.frankfurt-postgres.render.com/finespresso'#os.getenv('DATABASE_URL')

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class News(Base):
    __tablename__ = 'news'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    link = Column(Text)
    company = Column(Text)
    published_date = Column(TIMESTAMP(timezone=True))
    published_date_gmt = Column(TIMESTAMP(timezone=True))
    content = Column(Text)
    ai_summary = Column(Text)
    publisher_summary = Column(Text)
    ai_topic = Column(Text)
    industry = Column(Text)
    publisher_topic = Column(Text)
    publisher = Column(Text)
    downloaded_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    status = Column(String(255))
    mw_ticker = Column(String(255))
    yf_ticker = Column(String(255))
    ticker = Column(String(16))
    company_id = Column(Integer)
    timezone = Column(String(10))

def create_tables():
    Base.metadata.create_all(engine)

def add_news_items(news_items):
    session = Session()
    try:
        unique_items, duplicate_count = remove_duplicates(session, news_items)
        
        for item in unique_items:
            item.downloaded_at = datetime.utcnow()
        
        session.add_all(unique_items)
        session.commit()
        
        logging.info(f"Successfully added {len(unique_items)} news items to the database.")
        logging.info(f"Skipped {duplicate_count} duplicate items.")
        
        return len(unique_items), duplicate_count
    except Exception as e:
        logging.error(f"An error occurred while adding news items: {e}")
        session.rollback()
        return 0, 0
    finally:
        session.close()

def remove_duplicates(session, news_items):
    unique_items = []
    duplicate_count = 0
    
    for item in news_items:
        is_duplicate = session.query(exists().where(News.link == item.link)).scalar()
        
        if not is_duplicate:
            unique_items.append(item)
        else:
            duplicate_count += 1
    
    logging.info(f"Found {duplicate_count} duplicate items")
    logging.info(f"Keeping {len(unique_items)} unique items")
    
    return unique_items, duplicate_count

def map_to_db(df, source):
    logging.info(f"Mapping dataframe to News objects for source: {source}")
    
    news_items = []
    for _, row in df.iterrows():
        news_item = News(
            title=row['title'],
            link=row['link'],
            company=row['company'],
            published_date=row['published_date'],
            published_date_gmt=row['published_date_gmt'],
            publisher_topic=row['publisher_topic'],
            publisher=row['publisher'],
            downloaded_at=datetime.utcnow(),
            status=row['status'],
            content=row['content'],
            industry=row['industry'],
            ticker=row['ticker'],
            timezone=row['timezone'],
            ai_summary=row['ai_summary'],
            publisher_summary=row['publisher_summary']
        )

        if source == 'euronext':
            news_item.industry = row['industry']

        if row['publisher'] == 'ai':
            news_item.ai_summary = row['ai_summary']
            news_item.ai_topic = row['ai_topic']

        news_items.append(news_item)
    
    logging.info(f"Created {len(news_items)} News objects")
    
    return news_items

def remove_duplicate_news():
    session = Session()
    try:
        subquery = session.query(News.link, func.min(News.downloaded_at).label('min_downloaded_at')) \
                          .group_by(News.link) \
                          .subquery()
        
        duplicates = session.query(News.id) \
                            .join(subquery, and_(News.link == subquery.c.link,
                                                 News.downloaded_at != subquery.c.min_downloaded_at))
        
        deleted_count = session.query(News).filter(News.id.in_(duplicates)).delete(synchronize_session='fetch')
        
        updated_count = session.query(News).filter(News.status == 'raw').update({News.status: 'clean'}, synchronize_session='fetch')
        
        session.commit()
        logging.info(f"Successfully removed {deleted_count} duplicate news items.")
        logging.info(f"Updated status to 'clean' for {updated_count} news items.")
        
        return deleted_count, updated_count
    except Exception as e:
        logging.error(f"An error occurred while removing duplicates and updating status: {e}")
        session.rollback()
        return 0, 0
    finally:
        session.close()

def get_news_df(publishers, start_date, end_date):
    session = Session()
    try:
        query = select(News).where(
            News.publisher.in_(publishers),
            News.published_date >= start_date,
            News.published_date <= end_date
        ).order_by(News.published_date.desc())
        
        result = session.execute(query)
        news_items = result.scalars().all()
        
        data = [{
            'news_id': item.id,
            'ticker': item.ticker,
            'title': item.title,
            'link': item.link,
            'published_date': item.published_date,
            'company': item.company,
            'ai_topic': item.ai_topic,
            'ai_summary': item.ai_summary,
            'publisher': item.publisher
        } for item in news_items]
        
        return pd.DataFrame(data)
    finally:
        session.close()

def get_news_without_tickers():
    logging.info("Retrieving news items without MW tickers from database")
    
    session = Session()
    try:
        query = select(News).where(News.mw_ticker.is_(None))
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} news items without MW tickers")
        
        return news_items
    finally:
        session.close()

def update_news_tickers(news_items_with_tickers):
    logging.info("Updating database with extracted tickers")
    
    session = Session()
    try:
        updated_count = 0
        total_items = len(news_items_with_tickers)
        for index, (news_id, ticker) in enumerate(news_items_with_tickers):
            if ticker:
                stmt = update(News).where(News.id == news_id).values(mw_ticker=ticker)
                session.execute(stmt)
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                session.commit()
                logging.info(f"Processed {index + 1}/{total_items} items")
        
        logging.info(f"Successfully updated {updated_count} news items with tickers")
    except Exception as e:
        logging.error(f"Error updating tickers: {str(e)}")
        session.rollback()
    finally:
        session.close()

def update_news_status(news_ids, new_status):
    logging.info(f"Updating status to '{new_status}' for {len(news_ids)} news items")
    
    session = Session()
    try:
        updated_count = session.query(News).filter(News.id.in_(news_ids)).update({News.status: new_status}, synchronize_session='fetch')
        session.commit()
        logging.info(f"Successfully updated status for {updated_count} news items")
        
        return updated_count
    except Exception as e:
        logging.error(f"An error occurred while updating news status: {e}")
        session.rollback()
        return 0
    finally:
        session.close()

def get_news_without_company(publisher):
    logging.info(f"Retrieving news items without company names for publisher: {publisher}")
    
    session = Session()
    try:
        query = select(News).where(
            News.company.is_(None), 
            News.publisher == publisher
        )
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} news items without company names for {publisher}")
        
        return news_items
    finally:
        session.close()

def update_companies(enriched_df):
    logging.info("Updating database with enriched company names")
    
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item and 'company' in row and row['company']:
                news_item.company = row['company']
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with company names")
    except Exception as e:
        logging.error(f"Error updating company names: {str(e)}")
        session.rollback()
    finally:
        session.close()
