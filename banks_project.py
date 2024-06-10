# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import numpy as np
import datetime
import logging
import csv

code_log = 'code_log.txt'

#url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

# Function to extract data from the web.
def extract(url, table_attribs):

    df = pd.DataFrame(columns=['Name', 'Market cap (US$ billion)'])
    count = 0

    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    

    for row in rows:
        if count <= 10:
            col = row.find_all('td')
            if len(col)!=0:

            # Extract bank name from the anchor tag
                names = row.find_all('a')
                for link in names:
                    name = link.text
                
                    data_dict = {'Rank': col[0].contents[0], 
                                 'Name': name,
                                 'Market cap (US$ billion)': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
                count+=1

        else:
            break
    return df        
df = extract('https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks', {'class': 'wikitable sortable mw-collapsible jquery-tablesorter mw-made-collapsible'})
#print(extract('https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks', {'class': 'wikitable sortable mw-collapsible jquery-tablesorter mw-made-collapsible'}))
#print(df)

# Function to transform data.
def transform():
    
    # Removing string data type in the column Market cap (US$ billion) and Rank so perfom the multiplication.
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].replace(r'\n', '', regex=True).astype(float)
    df['Rank'] = df['Rank'].str.replace('\n', '')

    def csv_to_dict(csv_file):
        # Initialize an empty list to store dictionaries
        data = []

        # Open the CSV file for reading
        with open(csv_file, 'r') as file:
            # Create a CSV DictReader oblect
            reader = csv.DictReader(file)

            # Iterate over each row in the CSV file
            for row in reader:
                # Append each row (dictionary) to the list
                data.append(row)

        return data

    csv_file = 'exchange_rate.csv'
    exchange_rate = csv_to_dict(csv_file)
    #print(exchange_rate)

    # Our data is a list of dictionary instead of just a dictionary, so we need to find each rate by iterating through each dictionary.
    # For GBP
    gbp_exchange_rate_dict = next(item for item in exchange_rate if item['Currency'] == 'GBP')
    gbp_exchange_rate = gbp_exchange_rate_dict['Rate']
    # Convert gbp_exchange_rate to float
    gbp_exchange_rate = float(gbp_exchange_rate)

    # For EUR
    eur_exchange_rate_dict = next(item for item in exchange_rate if item['Currency'] == 'EUR')
    eur_exchange_rate = gbp_exchange_rate_dict['Rate']
     # Convert gbp_exchange_rate to float
    eur_exchange_rate = float(eur_exchange_rate)

    # For INR
    inr_exchange_rate_dict = next(item for item in exchange_rate if item['Currency'] == 'INR')
    inr_exchange_rate = gbp_exchange_rate_dict['Rate']
     # Convert gbp_exchange_rate to float
    inr_exchange_rate = float(inr_exchange_rate)

    # Adding 3 different columns to the dataframe and round the resulting data to 2 decimal places.
    df['MC_GBP_Billion'] = [np.round(float(x) * gbp_exchange_rate, 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(float(x) * eur_exchange_rate, 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(float(x) * inr_exchange_rate, 2) for x in df['Market cap (US$ billion)']]
    # or
    #df['MC_GBP_Billion'] = df['Market cap (US$ billion)'].apply(lambda x: np.round(x * gbp_exchange_rate, 2))

    return df

cleaned_df = transform()
print(cleaned_df)

# Function to load the transformed data frame to a CSV file.
def load_to_csv():
    
    cleaned_df.to_csv(csv_path)

    return csv_path

load_to_csv() 

# Function to load the transformed data frame to an SQL database.
def load_to_db():

    conn = sqlite3.connect('banks.db')
    df.to_sql('Largest_banks', conn, if_exists='replace', index=False)
    conn.close()

    return 

load_to_db()

# Function to run queries on database.
def run_queries():
    pass