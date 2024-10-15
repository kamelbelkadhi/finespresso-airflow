from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from utils.news_db_util import Base, engine
from datetime import datetime
import logging

Session = sessionmaker(bind=engine)

class Signups(Base):
    __tablename__ = 'signups'

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    captured_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

def save_email(email):
    session = Session()
    try:
        new_signup = Signups(email=email)
        session.add(new_signup)
        session.commit()
        logging.info(f"Successfully added email: {email}")
        return True
    except Exception as e:
        logging.error(f"An error occurred while saving email: {e}")
        session.rollback()
        return False
    finally:
        session.close()
