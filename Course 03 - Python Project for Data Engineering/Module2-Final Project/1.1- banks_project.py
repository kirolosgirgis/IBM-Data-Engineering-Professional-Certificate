import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
ex_path = 'C:\\Users\kyoss\Desktop\COURSERA\IBM DATA ENGINEERING PROFESSIONAL\Course 03 - Python Project for Data Engineering\exchange_rate.csv'
output_path = 'c:\\Users\\kyoss\\Desktop\\COURSERA\\IBM DATA ENGINEERING PROFESSIONAL\\Course 03 - Python Project for Data Engineering/Largest_banks_data.csv'
db_name = 'Banks.db'
conn = sqlite3.connect(db_name)
table_name = 'Largest_banks'
table_attribs = ["Name","MC_USD_Billion"]
df_final = pd.DataFrame(columns=table_attribs)


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt","a") as f:
        f.write(timestamp + ', ' + message + '\n')
    
    print(timestamp + ', ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # Loading the webpage for webscraping
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # Scraping for required info
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {table_attribs[0]:col[1].text.strip(),
                         table_attribs[1]:float(col[2].text.strip())}
            df1 = pd.DataFrame(data_dict, index = [0])
            if df.empty:
                df=df1                
            else:
                df = pd.concat([df,df1], ignore_index = True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # extract the exchange rate.csv file
    ex_df = pd.read_csv(ex_path, index_col=0)


    # create bew columns MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * ex_df.loc['GBP','Rate'],2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * ex_df.loc['EUR','Rate'],2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * ex_df.loc['INR','Rate'],2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    sql_connection.close()

def run_query(query_statement):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(query_statement)
    x = cursor.fetchall()
    conn.commit()
    conn.close()
    result = pd.DataFrame(x, columns=["#","Name","MC_USD_Billion","MC_EUR_Billion","MC_INR_Billion"])
    print(result)





''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("ETL Job Started")

# Extracting the data
log_progress("Extract phase Started")
extracted_data = extract(url,table_attribs)
log_progress("Extract phase Ended")

# Transforming the data
log_progress("Transform phase Started")
transformed_data = transform(extracted_data, ex_path)
log_progress("Transform phase Ended")

# Loading data to csv
log_progress("Load phase Started")
load_to_csv(transformed_data, output_path)
load_to_db(transformed_data, conn, table_name)
log_progress("Load phase Ended")

# Running SQL query
log_progress("Access SQL Program")

xx = 'y'
while xx == 'y':
    xx = input("Do you want to Run SQL Query?(y/n): ").lower()
    if xx == "y":
        query = input("Please Enter the Query Statment: ") 
        run_query(query)
        continue
    elif xx == "n":
        break
    else:
        print("Wrong Input!! Please answer y/n")
        xx="y"
        continue     

log_progress("Close SQL Program")

log_progress("ETL Job Ended")
