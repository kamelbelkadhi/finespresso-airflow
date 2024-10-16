import logging
import requests
from datetime import date

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
publisher_parser = {'XOSL': 'Oslo Bors', 'XOAX':'Euronext Expand',  'XOAM':'Nordic ABM', 'MERK':'Euronext Growth (Oslo)'}

class NewsReader:
    def __init__(self):
        self.base_url = "https://api3.oslo.oslobors.no/v1/newsreader"
        self.headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-GB,en;q=0.9,fr-TN;q=0.8,fr;q=0.7,ar-TN;q=0.6,ar;q=0.5,en-US;q=0.4",
            "content-type": "application/json",
            "origin": "https://newsweb.oslobors.no",
            "referer": "https://newsweb.oslobors.no/",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"
        }

    def get_news_list(self, from_date, to_date):
        """Fetch the list of news for the given date range."""
        url = f"{self.base_url}/list"
        params = {
            'category': '',
            'issuer': '',
            'fromDate': from_date,
            'toDate': to_date,
            'market': '',
            'messageTitle': ''
        }

        response = requests.post(url, headers=self.headers, params=params)

        if response.status_code == 200:
            return response.json().get('data', {}).get('messages', [])
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return []

    def get_news_details(self, message_id):
        """Fetch the detailed information for a specific news message."""
        url = f"{self.base_url}/message"
        params = {
            'messageId': message_id
        }

        response = requests.post(url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json().get('data', {}).get('message', {})
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {}

    def extract_text_from_pdf(self, pdf_content):
        """Extract text from a PDF file."""
        reader = PdfReader(BytesIO(pdf_content))
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text

    def extract_attachment_text(self, message_id, attachment):
        """Extract text from an attachment given its message ID and attachment info."""
        url = f"{self.base_url}/attachment"
        params = {
            'messageId': message_id,
            'attachmentId': attachment['id']
        }

        response = requests.get(url, headers=self.headers, params=params, stream=True)

        if response.status_code == 200:
            pdf_content = response.content
            return self.extract_text_from_pdf(pdf_content)
        else:
            print(f"Failed to retrieve attachment {attachment['name']}. Status code: {response.status_code}")
            return None

    def fetch_news_for_date(self, date):
        """Fetch all news and their details for a specific date, and extract any text from attachments."""
        news_list = self.get_news_list(from_date=date, to_date=date)
        detailed_news = []

        for news in news_list:
            news_details = self.get_news_details(news['id'])
            news_details['publisher'] = ','.join([publisher_parser[pub] for pub in news_details['markets']])
            detailed_news.append({**news_details, 'link':f"https://newsweb.oslobors.no/message/{news_details['messageId']}"})

        return detailed_news

def get_news_details():

    reader = NewsReader()

    # Iterate over the past month
    day = date.today().strftime('%Y-%m-%d')
    logging.info(f"Fetching news for {day}")

    news_details = reader.fetch_news_for_date(day)
    return news_details