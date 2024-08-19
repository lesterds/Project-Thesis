import requests
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from time import sleep
import xml.etree.ElementTree as ET


class GetETNewsArticles:
    def __init__(self, ticker):
        self.ticker = ticker
        self.news = None
    
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
        
    def get_et_news(self):
        options = Options()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--start-maximized')
        options.page_load_strategy = 'eager'
        options.add_argument("--disable-search-engine-choice-screen")
        options.add_experimental_option("detach", True)
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument("--headless")
        
        xml_file = 'nifty50_companies.xml'
        article_links = []
        all_articles_content = []
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        
        # Make the driver
        service = webdriver.ChromeService(executable_path=r"C:\webdrivers\chromedriver.exe") # Enter the path of chromedriver
        driver = webdriver.Chrome(service=service, options=options)
        
        # for ticker in tqdm(self.tickers, desc="Fetching article content"):
        company_dets = self.parse_xml(xml_file, self.ticker)
        ticker_name = company_dets["Ticker_Name"]
        cid = company_dets["CID"]
            
        wait = WebDriverWait(driver, 20)   
        # Go to the website
        driver.get(f"https://economictimes.indiatimes.com/{ticker_name}/stocksupdate/companyid-{cid}.cms")
        
        # Accept Cookies
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))).click()
        # Scroll slightly down otherwise it will not click on news tab
        driver.execute_script("window.scrollTo(0, 200)")
        sleep(2)
        
        # Click until news tab is clicked
        while True:
            try:
                wait.until(EC.element_to_be_clickable((By.XPATH, '//span[@class="tab"][1]'))).click()
                break
            except:
                print("Error")
                sleep(2)

        # Keep this increase if response empty or slow network
        sleep(5)
        html_content = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="news"]'))).get_attribute("innerHTML")
        soup = BeautifulSoup(html_content, "html.parser")
        driver.quit()
        
        each_stories = soup.find_all('div', class_='eachStory')
        for story in each_stories:
            # Find the <a> tag within each "eachStory" element and extract the href attribute
            a_tag = story.find('a')
            if a_tag and 'href' in a_tag.attrs and a_tag['href'].endswith('.cms'):
                article_links.append(a_tag['href'])
                
        # Loop through each article link and extract details
        for link in article_links:
            article_details = self.extract_article_content(link, headers, self.ticker)
            if article_details:
                all_articles_content.append(article_details)
                
        if all_articles_content:
            self.news_data = pd.DataFrame(all_articles_content)