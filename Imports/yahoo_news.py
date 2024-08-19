import requests
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm
import pandas as pd
import time

class GetYahooNewsData:
    def __init__(self, tickers):
        self.tickers = tickers
        self.news_data = pd.DataFrame()

    def get_page(self, url, headers) :
        """ Download a webpage and return a beautiful soup doc"""
        response = requests.get(url, headers=headers)
        if not response.ok:
            print('Status code:', response.status_code)
            raise Exception('Failed to load page {}'.format(url))
        
        page_content = response.text
        doc = BeautifulSoup(page_content, 'html.parser')
        return doc
        
    def get_article_links(self, doc):
        """Extract all hrefs from elements with class 'yf-13p9sh2'"""
        articles = doc.find_all('a', class_='yf-13p9sh2')
        links = [article['href'] for article in articles if 'href' in article.attrs and article['href'].endswith('.html')]
        return links

    # Step 2: Extract content from each article link
    def extract_article_content(self, url, headers, ticker):
        """Extract content from a news article"""
        response = requests.get(url, headers=headers)
        if not response.ok:
            print(f'Failed to fetch article: {url}')
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        
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
            return {'url': url, 'title': title, 'date': date, 'content': article_content, 'ticker': ticker}
        else:
            return None

    def get_yahoo_news(self):
        # Initialize an empty list to store the articles' details
        all_articles_content = []
        
        for ticker in tqdm(self.tickers["Stock"], desc="Fetching article content"):
            my_url = f'https://finance.yahoo.com/quote/{ticker}.NS/news?p={ticker}.NS'
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            
            document_dets = self.get_page(my_url, headers)
            
            # article_links = self.get_article_links(document_dets)
            articles = document_dets.find_all('a', class_='subtle-link')
            article_links = [article['href'] for article in articles if 'href' in article.attrs and article['href'].endswith('.html')]

            # Loop through each article link and extract details
            for link in article_links:
                article_details = self.extract_article_content(link, headers, ticker)
                if article_details:
                    all_articles_content.append(article_details)
            
            time.sleep(300)
            
        if all_articles_content:
            self.news_data = pd.DataFrame(all_articles_content)
                