import os
from openai import OpenAI

client = OpenAI(api_key="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")


tag_list = [
    "shares_issue", "observation_status", "financial_results", "mergers_acquisitions",
    "annual_general_meeting", "management_change", "annual_report", "exchange_announcement",
    "ex_dividend_date", "converence_call_webinar", "geographic_expansion", "analyst_coverage",
    "financial_calendar", "share_capital_increase", "bond_fixing", "fund_data_announcement",
    "capital_investment", "calendar_of_events", "voting_rights", "law_legal_issues",
    "initial_public_offering", "regulatory_filings", "joint_venture", "partnerships",
    "environmental_social_governance", "business_contracts", "financing_agreements", "patents",
    "bankruptcy",
    "lawsuit" ]
# Load environment variables

model_name = "gpt-4o-mini"  # Updated model name

def tag_news(news):
    prompt = f'Answering with one tag only, pick up the best tag which describes the news "{news}" from the list: {",".join(tag_list)}'
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