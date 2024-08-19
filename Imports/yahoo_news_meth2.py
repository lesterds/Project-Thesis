import html
from requests_html import HTMLSession
import re
import pandas as pd
from tqdm.notebook import tqdm
from bs4 import BeautifulSoup

class GetYahooNewsData2:
    def __init__(self, tickers):
        self.tickers = tickers
        self.news_data = pd.DataFrame()
        self.session = HTMLSession()

    # Step 2: Extract content from each article link
    def extract_article_content(self, ticker, url):
        all_article_content = []
        for article_link in url:
            article_resp = self.session.get(article_link).content.decode("utf-8")
            soup = BeautifulSoup(article_resp, 'html.parser')
            
            # Extract title
            title_tag = soup.find('h1', {'data-test-locator': 'headline'})
            title = title_tag.get_text() if title_tag else 'No title available'
            
            # Extract date
            date_tag = soup.find('time')
            date = date_tag['datetime'] if date_tag else 'No date available'
            
            # Extract article content
            article_body = soup.find('div', class_='caas-body')
            if article_body:
                paragraphs = article_body.find_all('p')
                article_content = ' '.join([p.text for p in paragraphs])
              
            article_details = {
                    'url': article_link,
                    'title': title,
                    'date': date,
                    'content': article_content,
                    'ticker': ticker
                }  
              
            # article_details = {
            #         'url': article_link,
            #         'title': re.findall(r'<title>(.*?)</title>', article_resp, re.IGNORECASE),
            #         'date': re.findall(r'<time[^>]*datetime="([^"]+)"', article_resp),
            #         'content': re.findall(r'<p[^>]*>.*?</p>', article_resp, re.DOTALL),
            #         'ticker': ticker
            #     }
            all_article_content.append(article_details)
            
        return all_article_content
        
    def get_yahoo_news2(self):
            all_articles_content = []
            article_links = []
            
            headers = {
                "Accept": "application/json, text/plain, /",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
                "Referer": "https://finance.yahoo.com/"
                }
            
            for ticker in tqdm(self.tickers["Stock"], desc="Fetching article content"):
                url = f'https://finance.yahoo.com/quote/{ticker}.NS/news?p={ticker}.NS'
                response = self.session.get(url).content.decode("utf-8")

                #Extract news article links
                links = re.findall(r'<a\s[^>]*class="[^"]*subtle-link[^"]*"[^>]*href="([^"]*\.html)"', response)
                # Define unwanted URLs
                unwanted_urls = [
                    'https://policies.oath.com/us/en/oath/privacy/adinfo/index.html'
                ]

                # Filter out unwanted URLs
                filtered_links = [link for link in links if link not in unwanted_urls]

                # Append the filtered links to article_links
                article_links.append((ticker, filtered_links))
                
            for ticker, links in tqdm(article_links, desc="Getting contents of the articles"):
                all_articles_content.extend(self.extract_article_content(ticker, links))
                
            if all_articles_content:    
                self.news_data = pd.DataFrame(all_articles_content)