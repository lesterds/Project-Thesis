import pandas as pd
from sklearn.preprocessing import StandardScaler
import re
from datetime import datetime

class PreprocessData:
    def __init__(self, stock_data, index_data, etf_data, news_data):
        self.stock_data = stock_data
        self.index_data = index_data
        self.etf_data = etf_data
        self.news_data = news_data

    def fill_missing_values(self):
        # Fill missing values for financial data
        self.stock_data.fillna(method='ffill', inplace=True)
        self.index_data.fillna(method='ffill', inplace=True)
        self.etf_data.fillna(method='ffill', inplace=True)

        # Fill missing values for news data
        self.news_data['date'].fillna(method='ffill', inplace=True)
        self.news_data['title'].fillna('No Title', inplace=True)
        self.news_data['content'].fillna('No Content', inplace=True)

    def remove_commas_and_convert_to_float(self, df, columns):
        for col in columns:
            if col in df.columns:
                # df[col] = df[col].astype(str)
                df[col] = df[col].str.replace(',', '').astype(float)
        return df

    def standardize_data(self):
        # Standardize numerical features
        scaler = StandardScaler()
        
        stock_numerical_cols = ['ClosePrice', 'TotalTradedQuantity', 'Open Interest', 'Option Close Price', 'Implied Volatility']
        index_numerical_cols = ['CLOSING_PRICE', 'TOT_TRADED_QTY', 'OPEN_INT', 'SETTLE_PRICE']
        etf_numerical_cols = ['ClosePrice', 'TotalTradedQuantity', 'Open Interest', 'Option Close Price', 'Implied Volatility']

        # Remove commas and convert to float
        self.stock_data = self.remove_commas_and_convert_to_float(self.stock_data, stock_numerical_cols)
        self.index_data = self.remove_commas_and_convert_to_float(self.index_data, index_numerical_cols)
        self.etf_data = self.remove_commas_and_convert_to_float(self.etf_data, etf_numerical_cols)

        # Filter existing columns
        stock_numerical_cols = [col for col in stock_numerical_cols if col in self.stock_data.columns]
        index_numerical_cols = [col for col in index_numerical_cols if col in self.index_data.columns]
        etf_numerical_cols = [col for col in etf_numerical_cols if col in self.etf_data.columns]

        if stock_numerical_cols:
            self.stock_data[stock_numerical_cols] = scaler.fit_transform(self.stock_data[stock_numerical_cols])

        if index_numerical_cols:
            self.index_data[index_numerical_cols] = scaler.fit_transform(self.index_data[index_numerical_cols])

        if etf_numerical_cols:
            self.etf_data[etf_numerical_cols] = scaler.fit_transform(self.etf_data[etf_numerical_cols])

    def encode_categorical_data(self):
        # Encode categorical features
        if 'Option Type' in self.stock_data.columns:
            self.stock_data = pd.get_dummies(self.stock_data, columns=['Option Type'])
        if 'OPTION_TYPE' in self.index_data.columns:
            self.index_data = pd.get_dummies(self.index_data, columns=['OPTION_TYPE'])
        if 'Option Type' in self.etf_data.columns:
            self.etf_data = pd.get_dummies(self.etf_data, columns=['Option Type'])

    def clean_text(self, text):
        text = re.sub(r'\s+', ' ', text)  # Remove extra spaces and newline characters
        return text.strip()
    
    def preprocess_date(self, date_str):
        try:
            if 'Last Updated:' in date_str:
                # Handle the first type of date format
                date_str = date_str.replace('Last Updated:', '').strip()
                date_obj = datetime.strptime(date_str, '%b %d, %Y, %I:%M:%S %p IST')
            else:
                # Handle the second type of date format
                date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            
            # Convert to the desired format dd-mm-yyyy
            return date_obj.strftime('%d-%m-%Y')
        except Exception as e:
            print(f"Error parsing date: {date_str}, Error: {e}")
            return None
    
    def preprocess_news_data(self):
        # Preprocess news data (if required)
        self.news_data['Clean_Title'] = self.news_data['title'].apply(lambda x: self.clean_text(str(x)))
        # Remove duplicate records based on 'title' and 'content'
        self.news_data.drop_duplicates(subset=['title', 'content'], inplace=True)
        # Process date column
        # self.news_data['date'] = self.news_data['date'].apply(self.preprocess_date)
    
    def preprocess(self):
        self.preprocess_news_data()
        self.fill_missing_values()
        # self.standardize_data()
        self.encode_categorical_data()
        
        return self.stock_data, self.index_data, self.etf_data, self.news_data
