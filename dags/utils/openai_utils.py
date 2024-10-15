import os
from openai import OpenAI
from dotenv import load_dotenv
from gptcache import cache

load_dotenv()

client = OpenAI()
cache.init()
cache.set_openai_key()

# Load environment variables

model_name = "gpt-4o"  # Updated model name

def tag_news(news, tags):
    prompt = f'Answering with one tag only, pick up the best tag which describes the news "{news}" from the list: {tags}'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    tag = response.choices[0].message.content
    return tag

def summarize(news):
    prompt = f'Summarize this in a brief, concise and neutral way like a financial analyst (50 words or less): "{news}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    summary = response.choices[0].message.content
    return summary

def extract_ticker(company):
    prompt = f'Extract the company or issuer ticker symbol corresponding to the company name provided. Return only the ticker symbol in uppercase, without any additional text. If you cannot assign a ticker symbol, return "N/A". Company name: "{company}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    ticker = response.choices[0].message.content.strip().upper()
    return ticker if ticker != "N/A" else None

def extract_issuer(news):
    prompt = f'Extract the company or issuer name corresponding to the text provided. Return concise entity name only. If you cannot assign a ticker symbol, return "N/A". News: "{news}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    ticker = response.choices[0].message.content.strip().upper()
    return ticker if ticker != "N/A" else None