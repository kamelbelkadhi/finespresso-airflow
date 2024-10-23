import logging
from utils.openai_utils import summarize, tag_news

def process_content_and_tags(news_row):
    """
    Process the content of a news row to generate AI summaries and tags.
    
    :param news_row: The row data containing the news content and title.
    :return: A dictionary of fields to update.
    """
    title = news_row['title']
    content = news_row['content']
    if not content:
    	return None
    
    fields_to_update = {}
    
    logging.info(f"Generating summary for news title: {title}")
    summary = summarize(content).replace('\n', '')
    logging.info(f"Generated summary: {summary}")
    
    logging.info(f"Generating tag for news title: {title}")
    tag = tag_news(content)
    logging.info(f"Generated tag: {tag}")
    
    if summary:
        fields_to_update['ai_summary'] = summary.split('&&&')[0]
        fields_to_update['ai_summary_ee'] = summary.split('&&&')[1]
    if tag:
        fields_to_update['publisher_topic'] = tag
    
    return fields_to_update
