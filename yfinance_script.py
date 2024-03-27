import yfinance as yf
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_stock_data(tickers, start_date):
    stock_data = {}
    try:
        for ticker in tickers:
            stock_data[ticker] = yf.download(ticker, start=start_date)
        return stock_data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def insert_data_into_db(conn, data, table_name):
    try:
        cursor = conn.cursor()
        for ticker, df in data.items():
            for index, row in df.iterrows():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = %s AND date = %s", (ticker, index.strftime('%Y-%m-%d')))
                count = cursor.fetchone()[0]
                if count == 0:
                    cursor.execute(f"INSERT INTO {table_name} (id, ticker, date, open, high, low, close, adj_close, volume) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (ticker, index.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume']))
        conn.commit()
        print("Data successfully inserted into database!")
    except Error as e:
        conn.rollback()
        print(f"Error inserting data into database :(: {e}")

def main():
    tickers = ["CMG", "MCD", "SBUX", "SHAK", "DPZ", "PZZA"]
    start_date = "2023-12-01"
    table_name = "skfinance"

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        stock_data = fetch_stock_data(tickers, start_date)

        if stock_data is not None:
            insert_data_into_db(conn, stock_data, table_name)
        else:
            print("Failed to fetch data")

    except Error as e:
        print(f"Error connecting to db: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Db connection closed")

if __name__ == "__main__":
    main()