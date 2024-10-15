import logging
import pandas as pd
from utils.web_util import fetch_url_content
from utils.openai_util import summarize, tag_news
from utils.tag_util import tags


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def enrich_tag_from_url(df):
    print("Starting enrichment process from URLs")
    logging.info("Starting enrichment process from URLs")
    
    def fetch_and_tag(row):
        try:
            content = fetch_url_content(row['link'])
            ai_topic = tag_news(content, tags)
            print(f"Generated tag for: {row['link']} - Tag: {ai_topic}")
            logging.info(f"Generated tag for: {row['link']} - Tag: {ai_topic}")
            return ai_topic
        except Exception as e:
            print(f"Error processing {row['link']}: {str(e)}")
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return None
    
    df['ai_topic'] = df.apply(fetch_and_tag, axis=1)
    print(f"Enrichment completed for {len(df)} items")
    logging.info(f"Enrichment completed for {len(df)} items")
    return df

def enrich_summary_from_url(df):
    logging.info("Starting enrichment process from URLs")
    def fetch_and_summarize(row):
        try:
            content = fetch_url_content(row['link'])
            ai_summary = summarize(content)
            logging.info(f"Generated summary for: {row['link']} (first 50 chars): {ai_summary[:50]}...")
            return ai_summary
        except Exception as e:
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return None
    
    df['ai_summary'] = df.apply(fetch_and_summarize, axis=1)
    logging.info(f"Enrichment completed for {len(df)} items")
    return df

def enrich_from_content(df):
    logging.info("Starting enrichment process from existing content")

    def apply_tag(row):
        try:
            if pd.notna(row['content']) and row['content']:
                ai_topic = tag_news(row['content'], tags)
                logging.info(f"AI topic for {row['link']}: {ai_topic}")
                return ai_topic
            else:
                logging.warning(f"No content available for tagging: {row['link']}")
                return "No content available for tagging"
        except Exception as e:
            logging.error(f"Error tagging news for {row['link']}: {str(e)}")
            return f"Error in tagging: {str(e)}"

    def apply_summary(row):
        try:
            if pd.notna(row['content']) and row['content']:
                ai_summary = summarize(row['content'])
                logging.info(f"Generated AI summary for {row['link']} (first 50 chars): {ai_summary[:50]}...")
                return ai_summary
            else:
                logging.warning(f"No content available for summarization: {row['link']}")
                return "No content available for summarization"
        except Exception as e:
            logging.error(f"Error summarizing news for {row['link']}: {str(e)}")
            return f"Error in summarization: {str(e)}"

    df['ai_topic'] = df.apply(apply_tag, axis=1)
    df['ai_summary'] = df.apply(apply_summary, axis=1)
    
    logging.info(f"Enrichment from content completed for {len(df)} items")
    return df

def enrich_all(df):
    logging.info("Starting full enrichment process")
    df = enrich_from_url(df)
    #df = enrich_from_content(df)
    logging.info(f"Full enrichment completed. DataFrame now has {len(df.columns)} columns")
    return df

def enrich_content_from_url(df):
    logging.info("Starting content enrichment from URLs")
    
    def fetch_and_enrich(row):
        try:
            content = fetch_url_content(row['link'])
            ai_summary = summarize(content)
            #ai_topic = tag_news(content, tags)
            logging.info(f"Enriched content for: {row['link']}")
            print(f"Enriched content for: {row['link']}")
            return pd.Series({'content': content, 'ai_summary': ai_summary})
        except Exception as e:
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return pd.Series({'content': None, 'ai_summary': None})
    
    enriched = df.apply(fetch_and_enrich, axis=1)
    df = pd.concat([df, enriched], axis=1)
    logging.info(f"Content enrichment completed for {len(df)} items")
    return df