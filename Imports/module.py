import pandas as pd
from nselib import derivatives, capital_market
from datetime import datetime, timedelta
import warnings
import requests
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm
from serpapi import GoogleSearch
import xml.etree.ElementTree as ET

# Suppress the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

class GetFinData:
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.hist_dat = {}
        self.options_dat = {}
        self.news_data = pd.DataFrame()
        
    def get_past_data(self, ticker, asset_type):
        try:
            if asset_type == "Stock" or asset_type == "ETF":
                historical_data = capital_market.price_volume_and_deliverable_position_data(symbol=ticker, period='1Y')
            elif asset_type == "Index":
                historical_data = derivatives.future_price_volume_data(symbol=ticker, instrument='FUTIDX', from_date=self.start_date.strftime('%d-%m-%Y'), to_date=self.end_date.strftime('%d-%m-%Y'))
            df = pd.DataFrame(historical_data)
            if df.empty:
                print(f"No historical data found for {ticker}")
            df['Ticker'] = ticker
            return df
        except Exception as e:
            print(f"Error fetching historical data for {ticker}: {e}")
            return pd.DataFrame()     
    
    def get_options_data(self, ticker):
        try:
            options_data = derivatives.nse_live_option_chain(symbol=ticker)
            if options_data.empty:
                print(f"No options data found for {ticker}")
                return pd.DataFrame()

            records = []
            for i in range(len(options_data)):
                record_call = {
                    'Date': options_data.at[i, 'Fetch_Time'],
                    'Ticker': options_data.at[i, 'Symbol'],
                    'Option Type': 'call',
                    'Strike Price': options_data.at[i, 'Strike_Price'],
                    'Expiry Date': options_data.at[i, 'Expiry_Date'],
                    'Option Close Price': options_data.at[i, 'CALLS_LTP'],
                    'Implied Volatility': options_data.at[i, 'CALLS_IV'],
                    'Open Interest': options_data.at[i, 'CALLS_OI'],
                    'Volume': options_data.at[i, 'CALLS_Volume'],
                    'Delta': options_data.at[i, 'CALLS_Delta'] if 'CALLS_Delta' in options_data.columns else None,
                    'Gamma': options_data.at[i, 'CALLS_Gamma'] if 'CALLS_Gamma' in options_data.columns else None,
                    'Theta': options_data.at[i, 'CALLS_Theta'] if 'CALLS_Theta' in options_data.columns else None,
                    'Vega': options_data.at[i, 'CALLS_Vega'] if 'CALLS_Vega' in options_data.columns else None
                }
                records.append(record_call)
                
                record_put = {
                    'Date': options_data.at[i, 'Fetch_Time'],
                    'Ticker': options_data.at[i, 'Symbol'],
                    'Option Type': 'put',
                    'Strike Price': options_data.at[i, 'Strike_Price'],
                    'Expiry Date': options_data.at[i, 'Expiry_Date'],
                    'Option Close Price': options_data.at[i, 'PUTS_LTP'],
                    'Implied Volatility': options_data.at[i, 'PUTS_IV'],
                    'Open Interest': options_data.at[i, 'PUTS_OI'],
                    'Volume': options_data.at[i, 'PUTS_Volume'],
                    'Delta': options_data.at[i, 'PUTS_Delta'] if 'PUTS_Delta' in options_data.columns else None,
                    'Gamma': options_data.at[i, 'PUTS_Gamma'] if 'PUTS_Gamma' in options_data.columns else None,
                    'Theta': options_data.at[i, 'PUTS_Theta'] if 'PUTS_Theta' in options_data.columns else None,
                    'Vega': options_data.at[i, 'PUTS_Vega'] if 'PUTS_Vega' in options_data.columns else None
                }
                records.append(record_put)
            
            df = pd.DataFrame(records)
            if df.empty:
                print(f"No options data records for {ticker}")
            return df
        except Exception as e:
            print(f"Error fetching options data for {ticker}: {e}")
            return pd.DataFrame()
        
    def fetch_and_store_data(self, asset_type, tickers):
        historical_data_frames = []
        options_data_frames = []

        for ticker in tickers:
            hist_df = self.get_past_data(ticker, asset_type)
            if not hist_df.empty:
                hist_df['Asset Type'] = asset_type
                historical_data_frames.append(hist_df)
            
            if asset_type == 'Stock':
                opt_df = self.get_options_data(ticker)
                if not opt_df.empty:
                    opt_df['Asset Type'] = asset_type
                    options_data_frames.append(opt_df)

        if historical_data_frames:
            # Manually check for empty DataFrames before concatenation
            non_empty_hist_data_frames = [df for df in historical_data_frames if not df.empty]
            self.hist_dat[asset_type] = pd.concat(non_empty_hist_data_frames, ignore_index=True) if non_empty_hist_data_frames else pd.DataFrame()
        
        if options_data_frames:
            # Manually check for empty DataFrames before concatenation
            non_empty_options_data_frames = [df for df in options_data_frames if not df.empty]
            self.options_dat[asset_type] = pd.concat(non_empty_options_data_frames, ignore_index=True) if non_empty_options_data_frames else pd.DataFrame()

    def run(self):
        for asset_type, ticker_list in self.tickers.items():
            self.fetch_and_store_data(asset_type, ticker_list)

    def get_data(self, asset_type):
        return self.hist_dat.get(asset_type, pd.DataFrame()), self.options_dat.get(asset_type, pd.DataFrame())

    def get_news_data(self, ticker):
        try:
            url = f"https://www.google.com/finance/quote/{ticker}:NSE?sa=X&ved=2ahUKEwiDhJPLw5LzAhUhyzgGHYzqBDQQ3ecFegQINBAH"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            articles = soup.find_all('div', class_='yY3Lee')
            news_data = []
            for article in articles:
                title = article.find('a').get_text()
                description = article.find('div', class_='Adak').get_text()
                article_url = "https://www.google.com" + article.find('a')['href']
                date = article.find('time')['datetime'][:10] if article.find('time') else None
                
                news_data.append({
                    'Date': date,
                    'Title': title,
                    'Description': description,
                    'URL': article_url,
                    'Ticker': ticker
                })
            
            return pd.DataFrame(news_data)
        except Exception as e:
            print(f"Error fetching news data for {ticker} from Google Finance: {e}")
            return pd.DataFrame()
    
    # Function to parse XML and extract company details
    def parse_xml(self, xml_file, ticker):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for company in root.findall('Company'):
            company_ticker = company.find('Ticker').text
            if company_ticker == ticker:
                ticker_name = company.find('Ticker_Name').text
                cid = company.find('CID').text
                return {'Ticker_Name': ticker_name, 'CID': cid}
        
        return None
    
    # Function to construct the Economic Times URL
    def construct_url(self, company_dets):
        base_url = "https://economictimes.indiatimes.com"
        ticker_name = company_dets["Ticker_Name"]
        cid = company_dets["CID"]
        url = f"{base_url}/{ticker_name}/stocks/companyid-{cid}.cms"
        return url
    
    def get_page(self, url, headers) :
        """ Download a webpage and return a beautiful soup doc"""
        response = requests.get(url, headers=headers)
        if not response.ok:
            print('Status code:', response.status_code)
            raise Exception('Failed to load page {}'.format(url))
        
        page_content = response.text
        doc = BeautifulSoup(page_content, 'html.parser')
        return doc
    
    # Step 2: Extract content from each article link
    def extract_article_content(self, uri, headers, ticker):
        base_url = "https://economictimes.indiatimes.com"
        url = f"{base_url}{uri}"
        """Extract content from a news article"""
        response = requests.get(url, headers=headers)
        if not response.ok:
            print(f'Failed to fetch article: {url}')
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title_tag = soup.find('h1', class_='artTitle')
        title = title_tag.get_text() if title_tag else 'No title available'
        
        # Extract date
        date_tag = soup.find('time', class_='jsdtTime')
        date = date_tag.get_text() if date_tag else 'No date available'
        
        # Extract article content
        article_body = soup.find('h2', class_='summary')
        if article_body:
            article_content = article_body.get_text()
            # paragraphs = article_body.find_all('p')
            # article_content = ' '.join([p.text for p in paragraphs])
            return {'url': url, 'title': title, 'date': date, 'content': article_content, 'ticker': ticker}
        else:
            return None
    
    def gather_news(self, batch):
        # all_news_data = []
        # for ticker in self.tickers["Stock"]:  # Assuming we're only gathering news for stocks
        #     news_df = self.get_news_data(ticker)
        #     if not news_df.empty:
        #         all_news_data.append(news_df)
        
        # if all_news_data:
        #     self.news_data = pd.concat(all_news_data, ignore_index=True)
        # else:
        #     print("No news data to gather.")
        
        # Initialize an empty list to store the articles' details
        article_links = []
        all_articles_content = []
        
        # Parse the XML file
        xml_file = 'nifty50_companies.xml'
        
        for ticker in tqdm(batch, desc="Fetching article content"):
            company_dets = self.parse_xml(xml_file, ticker)
            print(ticker)
            url = self.construct_url(company_dets)
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            
            response = requests.get(url, headers=headers)
            # print("response.ok : {} , response.status_code : {}".format(response.ok , response.status_code))
            soup = BeautifulSoup(response.content, 'html.parser')            
            a_tag = soup.find('a', class_='full_btn')
            if a_tag and 'href' in a_tag.attrs:
                ticker_url = a_tag['href']   
                document_dets = self.get_page(ticker_url, headers)
                               
                each_stories = document_dets.find_all('div', class_='eachStory')
                for story in each_stories:
                    # Find the <a> tag within each "eachStory" element and extract the href attribute
                    a_tag = story.find('a')
                    if a_tag and 'href' in a_tag.attrs and a_tag['href'].endswith('.cms'):
                        article_links.append(a_tag['href'])
                        
                # Loop through each article link and extract details
                for link in article_links:
                    article_details = self.extract_article_content(link, headers, ticker)
                    if article_details:
                        all_articles_content.append(article_details)
                
        if all_articles_content:
            self.news_data = pd.DataFrame(all_articles_content)
        breakpoint
        
        # for ticker in tqdm(self.tickers["Stock"], desc="Fetching article content"):
        #     my_url = f'https://finance.yahoo.com/quote/{ticker}.NS/news?p={ticker}.NS'
        #     headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            
        #     document_dets = self.get_page(my_url, headers)
            
        #     # article_links = self.get_article_links(document_dets)
        #     articles = document_dets.find_all('a', class_='yf-13p9sh2')
        #     article_links = [article['href'] for article in articles if 'href' in article.attrs and article['href'].endswith('.html')]

        #     # Loop through each article link and extract details
        #     for link in article_links:
        #         article_details = self.extract_article_content(link, headers, ticker)
        #         if article_details:
        #             all_articles_content.append(article_details)
            
        # if all_articles_content:
        #     self.news_data = pd.DataFrame(all_articles_content)
        
        
    