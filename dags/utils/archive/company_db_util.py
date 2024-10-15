from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from utils.news_db_util import Base, engine

Session = sessionmaker(bind=engine)

class Company(Base):
    __tablename__ = 'company'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    ticker = Column(String(20))
    yf_ticker = Column(String(20))
    exchange = Column(String(50))
    exchange_code = Column(String(50))
    country = Column(String(50))
    mw_ticker = Column(String(255))
    yf_url = Column(String(255))
    mw_url = Column(String(255))

def save_company(df):
    session = Session()
    try:
        for _, row in df.iterrows():
            company = Company(
                yf_ticker=row['yf_ticker'],
                mw_ticker=row['mw_ticker'],
                yf_url=row['yf_url'],
                mw_url=row['mw_url']
            )
            session.add(company)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
