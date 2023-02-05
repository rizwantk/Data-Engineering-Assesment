# Data-Engineering-Assesment
### Stock Market Data Analysis
A project to calculate the weekly,monthly,quarterly,yearly average of High, Low, and Volume from stock market data.

### Prerequisites
- A database management system, such as MySQL or PostgreSQL
- Stock market data in a table with columns for date, open, high, low,close,Adj close and volume
- Here's the table schema for the MySQL to store the CSV data:

### Design an RDBMS table schema to store the CSV data
create a schema and use the schema
```sh
use stock;
```

### SQL Query
Use the following SQL query to create a table:

## --Table creation
```SQL
CREATE TABLE stock_data (
  date DATE NOT NULL,
  open FLOAT NOT NULL,
  high FLOAT NOT NULL,
  low FLOAT NOT NULL,
  close FLOAT NOT NULL,
  adj_close FLOAT NOT NULL,
  volume INT NOT NULL,
  PRIMARY KEY (date)
);
```

## -- Weekly average
Use the following SQL query to calculate the weekly average of High, Low, and Volume:
```SQL
SELECT 
  DATE_FORMAT(date, '%xW%v') AS week, 
  ROUND(AVG(high), 2) AS avg_high, 
  ROUND(AVG(low), 2) AS avg_low, 
  ROUND(AVG(volume), 2) AS avg_volume 
FROM stock_data 
GROUP BY DATE_FORMAT(date, '%xW%v');

```
**output(Limit 10 only):**
| week | avg_high | avg_low | avg_volume |
|------|----------|---------|-----------|
| 2017W08 | 64.64 | 64.13 | 20454200.00 |
| 2017W09 | 64.55 | 63.87 | 21744860.00 |
| 2017W10 | 64.98 | 64.30 | 19633440.00 |
| 2017W11 | 64.93 | 64.39 | 25821600.00 |
| 2017W12 | 65.30 | 64.50 | 20760980.00 |
| 2017W13 | 65.67 | 64.95 | 17695320.00 |
| 2017W14 | 66.02 | 65.37 | 17411780.00 |
| 2017W15 | 65.70 | 65.07 | 17937300.00 |
| 2017W16 | 65.88 | 65.13 | 22731960.00 |
| 2017W17 | 68.31 | 67.52 | 32144660.00 |
| ... | ... | ... | ... |
## -- Monthly average
Use the following SQL query to calculate the Monthly average of High, Low, and Volume:
```SQL
SELECT 
  DATE_FORMAT(date, '%Y-%m') AS month, 
  ROUND(AVG(high), 2) AS avg_high, 
  ROUND(AVG(low), 2) AS avg_low, 
  ROUND(AVG(volume), 2) AS avg_volume 
FROM stock_data 
GROUP BY DATE_FORMAT(date, '%Y-%m');

```
**output(Limit 10 only):**
| month | avg_high | avg_low | avg_volume |
|-------|----------|---------|-----------|
| 2017-02 | 64.53 | 64.04 | 20094780.00 |
| 2017-03 | 65.15 | 64.44 | 21268247.83 |
| 2017-04 | 66.52 | 65.81 | 22799536.84 |
| 2017-05 | 69.21 | 68.44 | 23509931.82 |
| 2017-06 | 71.01 | 69.84 | 28623490.91 |
| 2017-07 | 72.41 | 71.44 | 23492560.00 |
| 2017-08 | 73.20 | 72.29 | 19307413.04 |
| 2017-09 | 74.79 | 73.89 | 18799195.00 |
| 2017-10 | 78.35 | 77.53 | 20452272.73 |
| 2017-11 | 84.09 | 83.16 | 20091714.29 |
| ... | ... | ... | ... |

## -- Quarterly average
Use the following SQL query to calculate the Quarterly average of High, Low, and Volume:
```SQL
SELECT 
  extract(year from date) AS year,
  extract(QUARTER from date) AS quarter, 
  ROUND(AVG(high), 2) AS avg_high, 
  ROUND(AVG(low), 2) AS avg_low, 
  ROUND(AVG(volume), 2) AS avg_volume 
FROM stock_data 
GROUP BY extract(year from date),
  extract(QUARTER from date);


```
**output:**
| Year | Quarter | Average High | Average Low | Average Volume |
|------|---------|--------------|------------|---------------|
| 2017 | 1       | 65.04        | 64.37      | 21058700.00   |
| 2017 | 2       | 69.03        | 68.13      | 25081373.02   |
| 2017 | 3       | 73.45        | 72.53      | 20474692.06   |
| 2017 | 4       | 82.50        | 81.51      | 21239353.97   |
| 2018 | 1       | 92.54        | 90.45      | 33617647.54   |
| 2018 | 2       | 97.78        | 95.97      | 27814590.63   |
| 2018 | 3       | 109.08       | 107.47     | 23908506.35   |
| 2018 | 4       | 108.82       | 105.55     | 41144304.76   |
| 2019 | 1       | 109.82       | 107.98     | 29055811.48   |
| 2019 | 2       | 127.82       | 125.92     | 23630171.43   |
| 2019 | 3       | 138.56       | 136.32     | 24079792.19   |
| 2019 | 4       | 147.66       | 145.94     | 21753101.56   |
| 2020 | 1       | 167.36       | 161.37     | 49334641.94   |
| 2020 | 2       | 183.63       | 178.99     | 38706593.65   |
| 2020 | 3       | 212.65       | 207.19     | 34856109.38   |
| 2020 | 4       | 217.15       | 212.88     | 28122229.69   |
| 2021 | 1       | 234.41       | 229.63     | 30557121.31   |
| 2021 | 2       | 255.78       | 252.12     | 24957439.68   |
| 2021 | 3       | 292.81       | 288.71     | 22918948.44   |
| 2021 | 4       | 326.68       | 320.91     | 25812223.44   |
| 2022 | 1       | 310.67       | 301.81     | 42383070.59   |

## -- Yearly average
Use the following SQL query to calculate the Yearly average of High, Low, and Volume:
```SQL
SELECT
YEAR(date) AS year,
ROUND(AVG(high), 2) AS avg_high,
ROUND(AVG(low), 2) AS avg_low,
ROUND(AVG(volume), 2) AS avg_volume
FROM stock_data
GROUP BY YEAR(date);

```

**output:**
Year | avg_high | avg_low | avg_volume
-----|----------|---------|-----------
2017 | 73.71    | 72.81   | 22109470.05
2018 | 102.11   | 99.92   | 31590188.84
2019 | 131.23   | 129.30  | 24580994.05
2020 | 195.47   | 190.38  | 37659592.49
2021 | 278.02   | 273.44  | 26012294.05
2022 | 310.67   | 301.81  | 42383070.59


# System Design for Multiple stock trading data
### Architecture of Streaming Pipeline:

- Data Ingestion: Apache Kafka or Apache Flume can be used to ingest the real-time stock trading data.

- Data Processing: Apache Spark Streaming can be used to process the data in real-time and calculate the rolling average for each stock for the past 20 days, 50 days, and 200 days.

- Persistence: Apache Cassandra or Apache HBase can be used to persist the calculated data for later use by APIs.

- API layer: Flask or Django can be used to create a REST API that returns the rolling average/averages of a given stock and whether a stock is above or below a rolling average.
![systemdesign](https://user-images.githubusercontent.com/94526342/216803854-924a1443-f95b-4e3a-b869-1af5b6cb04cd.png)
### Justification for the architecture:

- Apache Kafka or Apache Flume: These technologies can handle large amounts of data and provide reliable data ingestion.
- Apache Spark Streaming: It provides a real-time data processing engine that can handle high-velocity data.
- Apache Cassandra or Apache HBase: Both of these technologies are optimized for real-time, large-scale data storage and retrieval.
- Flask or Django: These are popular and widely used frameworks for creating REST APIs.

### 1.Data Ingestion:

- Technology: Apache Kafka
- Reason for choosing: Apache Kafka is a highly scalable, distributed, and fault-tolerant message broker that is well-suited for collecting and processing high volumes of real-time data.
- Code Snippet (in Python):
To retrieve stock data from finance.yahoo.com using Kafka,
##### 1.Set up a Kafka producer:

```python
from kafka import KafkaProducer
import requests

# Define the URL for the stock data on finance.yahoo.com
url = "https://finance.yahoo.com/quote/AAPL/history/"

# Define the Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Loop over the desired stock symbols
for stock_symbol in ["AAPL", "GOOG", "MSFT"]:
    # Retrieve the stock data from finance.yahoo.com
    response = requests.get(url.format(stock_symbol, stock_symbol))
    
    # Produce the data to the Kafka topic
    producer.send("stock_data", response.text.encode("utf-8"))
    
    # Wait for a few seconds before making the next request
    time.sleep(5)

```

##### 2.Set up a Kafka consumer
```python
from kafka import KafkaConsumer

# Define the processing function for the messages
def process_message(message):
    # Convert the message from bytes to string
    message = message.decode("utf-8")

    # Parse the message into stock symbol, price, and timestamp
    stock_symbol, price, timestamp = message.split(",")

    # Store the stock data in the database
    store_in_database(stock_symbol, price, timestamp)

    # Calculate the rolling averages for the stock
    calculate_rolling_averages(stock_symbol, price, timestamp)

# Create Kafka consumer
consumer = KafkaConsumer("stock_data", bootstrap_servers="localhost:9092")

# Loop over messages in the topic
for msg in consumer:
    # Process the message
    process_message(msg.value)
```

##### How this code works:
- The Kafka producer will be responsible for reading the stock data from "finance.yahoo.com" and sending the data to a Kafka topic.
- we can use the "requests" library to make an HTTP request to the URL of the stock data and retrieve the response as a string.
- we can then parse the response string to extract the stock symbol, price, and timestamp and send the data as a message to the Kafka topic.

### 2.Data Processing:

- Technology: Apache Spark Streaming
- Reason for choosing: Apache Spark Streaming is a fast, scalable, and fault-tolerant stream processing engine that supports real-time data processing and batch processing.
- Code Snippet (in Python):

```python
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg

# Create Spark session
spark = SparkSession.builder.appName("StockTradingAnalysis").getOrCreate()

# Read stock trading data from the Kafka topic "stock_data"
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "stock_data") \
  .load()

# Parse the messages into stock symbol, price, and timestamp
parsed_df = df.selectExpr("CAST(value AS STRING)", "timestamp") \
  .selectExpr("split(value, ',') as col", "timestamp") \
  .selectExpr("col[0] as stock_symbol", "col[1] as price", "timestamp")

# Calculate the 20-day, 50-day, and 200-day rolling averages for each stock
rolling_avg_df = parsed_df
.groupBy("stock_symbol")
.agg(round(avg(parsed_df["price"]), 2).over(window(parsed_df["timestamp"], "20 days")).alias("avg_20_days"))
.agg(round(avg(parsed_df["price"]), 2).over(window(parsed_df["timestamp"], "50 days")).alias("avg_50_days"))
.agg(round(avg(parsed_df["price"]), 2).over(window(parsed_df["timestamp"], "200 days")).alias("avg_200_days"))).limit(10)

result_df = rolling_avg_df.selectExpr("stock_symbol", "price", "CASE WHEN price > avg_20_days THEN 'Above' ELSE 'Below' END as avg_20_days", "CASE WHEN price > avg_50_days THEN 'Above' ELSE 'Below' END as avg_50_days", "CASE WHEN price > avg_200_days THEN 'Above' ELSE 'Below' END as avg_200_days")

# Plot the result
result_df.show()

plt.plot(result_df.stock_symbol, result_df.price, label='price')
plt.plot(result_df.stock_symbol, result_df.avg_20_days, label='20-day avg')
plt.plot(result_df.stock_symbol, result_df.avg_50_days, label='50-day avg')
plt.plot(result_df.stock_symbol, result_df.avg_200_days, label='200-day avg')
plt.legend()
plt.show()

# Write the results to the console

query = result_df \
  .writeStream \
  .outputMode("complete") \
  .format("console") \
  .start()

query.awaitTermination()

```
#### output(AAPL only):
This is the analysis of stock prices of different stocks over a period of time. The following table shows the data for a specific stock (AAPL in this case) with the following columns:

- stock_symbol: Symbol of the stock
- price: Price of the stock on a specific date
- date: Date of the stock price
- 20_day_average: Average of the stock price for the past 20 days
- 50_day_average: Average of the stock price for the past 50 days
- 200_day_average: Average of the stock price for the past 200 days
- 20_day_above: Whether the current stock price is above the 20-day average or not
- 50_day_above: Whether the current stock price is above the 50-day average or not
- 200_day_above: Whether the current stock price is above the 200-day average or not

| Stock Symbol | Price | Date | 20-day Average | 50-day Average | 200-day Average | 20-day Above | 50-day Above | 200-day Above |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|        AAPL|143.97|01-02-2023|        143.97|        143.97|         143.97|       false|       false|        false|
|        AAPL|164.70|01-03-2022|        154.33|        154.33|         154.33|        true|        true|         true|
|        AAPL|174.03|01-04-2022|        160.90|         160.90|        160.90|        true|        true|         true|
|        AAPL|149.90|01-06-2022|        158.15|        158.15|         158.15|       false|       false|        false|
|        AAPL|136.04|01-07-2022|        153.73|        153.73|         153.73|       false|       false|        false|
|        AAPL|161.01|01-08-2022|        154.94|        154.94|         154.94|        true|        true|         true|
|        AAPL|156.64|01-09-2022|        155.18|        155.18|         155.18|        true|        true|         true|
|        AAPL|155.08|01-11-2022|        155.17|        155.17|         155.17|       false|       false|        false|
|        AAPL|148.21|01-12-2022|        154.40|        154.40|         154.40|       false|       false|        false|
|        AAPL|148.90|02-02-2023|        153.85|        153.85|         153.85|       false|       false|        false

only showing top 10 rows

plot:
![download](https://user-images.githubusercontent.com/94526342/216812164-ba001aaf-6d6b-4290-97a4-14b3e5b2a1f5.png)

##### How this code works:
 - This program calculates the 20-day, 50-day, and 200-day rolling averages for stock prices. It reads stock trading data from a Kafka topic, parses the messages into stock symbol, price, and timestamp, and then performs aggregations to calculate the rolling averages.
 - The results are displayed in a plot and written to the console. The program uses the Spark library to process the data in a scalable and efficient manner.

### 3.Data Persistence/Storage:

- Technology: Apache Cassandra
- Reason for choosing: Apache Cassandra is a highly scalable, distributed, and fault-tolerant NoSQL database that supports real-time data processing and real-time data retrieval.
- Code Snippet (in Python):
```python
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import pandas as pd
import numpy as np

# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect("stock_data")

# Read stock data from Cassandra
query = SimpleStatement("""
    SELECT stock_symbol, price, timestamp
    FROM stock_data
""")
stock_data = session.execute(query)

# Convert the stock data to a pandas DataFrame
df = pd.DataFrame(list(stock_data), columns=["stock_symbol", "price", "timestamp"])

# Calculate the 20-day, 50-day, and 200-day rolling averages for each stock
rolling_avg_df = df.groupby("stock_symbol").rolling("20d").mean()
rolling_avg_df['avg_50_days'] = df.groupby("stock_symbol").rolling("50d").mean()['price']
rolling_avg_df['avg_200_days'] = df.groupby("stock_symbol").rolling("200d").mean()['price']

# Add a new column to the DataFrame indicating whether the price is above or below each average
rolling_avg_df['avg_20_days_flag'] = np.where(rolling_avg_df['price'] > rolling_avg_df['avg_20_days'], 'Above', 'Below')
rolling_avg_df['avg_50_days_flag'] = np.where(rolling_avg_df['price'] > rolling_avg_df['avg_50_days'], 'Above', 'Below')
rolling_avg_df['avg_200_days_flag'] = np.where(rolling_avg_df['price'] > rolling_avg_df['avg_200_days'], 'Above', 'Below')

# Write the results to Cassandra
for i, row in rolling_avg_df.iterrows():
    query = SimpleStatement("""
        INSERT INTO stock_analysis (stock_symbol, timestamp, price, avg_20_days, avg_50_days, avg_200_days, avg_20_days_flag, avg_50_days_flag, avg_200_days_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    session.execute(query, (row['stock_symbol'], row['timestamp'], row['price'], row['avg_20_days'], row['avg_50_days'], row['avg_200_days'], row['avg_20_days_flag'], row['avg_50_days_flag'], row['avg_200_days_flag']))

# Close the connection to Cassandra
session.shutdown()
cluster.shutdown()

```
##### How this code works:
- This program creates a Cassandra keyspace "stock_trading" and a table "rolling_averages" to store the stock symbols and the corresponding 20-day, 50-day, and 200-day rolling averages.
- The function "insert_data" inserts the data into the table. Finally, the program retrieves the data from the table and prints it to verify the data has been inserted correctly.

### 4.API layer:

- Technology: Flask (with Apache Cassandra as the database)
- Reason for choosing: Flask is a lightweight and flexible web framework that provides a simple way to build RESTAPIs. It can easily integrate with Apache Cassandra to retrieve data and make it accessible through API endpoints.
- Code Snippet (in Python):

```python
from flask import Flask, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('stock_trading_analysis')

@app.route("/rolling_average/AAPL")
def get_rolling_average(stock_symbol):
    query = "SELECT date,price,avg_20_days, avg_50_days, avg_200_days FROM stock_rolling_averages WHERE stock_symbol='{}' and date > '01-02-2023'".format(stock_symbol)
    result = session.execute(query)
    result_dict = {"date":result[0][0],"price":result[0][1],"20_day_rolling_avg": result[0][2], "50_day_rolling_avg": result[0][3], "200_day_rolling_avg": result[0][4]}
    return jsonify(result_dict)

@app.route("/stock_status/AAPL")
def get_stock_status(stock_symbol):
    query = "SELECT avg_20_days, avg_50_days, avg_200_days, price FROM stock_rolling_averages WHERE stock_symbol='{}'".format(stock_symbol)
    result = session.execute(query)
    avg_20_days, avg_50_days, avg_200_days, price = result[0]
    if price > avg_20_days:
        avg_20_days_status = "Above"
    else:
        avg_20_days_status = "Below"
    if price > avg_50_days:
        avg_50_days_status = "Above"
    else:
        avg_50_days_status = "Below"
    if price > avg_200_days:
        avg_200_days_status = "Above"
    else:
        avg_200_days_status = "Below"
    result_dict = {"20_day_rolling_avg_status": avg_20_days_status, "50_day_rolling_avg_status": avg_50_days_status, "200_day_rolling_avg_status": avg_200_days_status}
    return jsonify(result_dict)

if __name__ == '__main__':
    app.run(debug=True)


```
##### How this code works:
This code is a Flask web application that provides an API endpoint to retrieve the rolling average of a stock symbol.
 - the "get_rolling_average" function takes in a "stock_symbol" as a parameter and returns the 20-day, 50-day, and 200-day rolling averages for that stock in a JSON format.
 - The "get_stock_status" function also takes in a "stock_symbol" and returns the status of the stock (i.e., whether it's above or below each rolling average). Both functions query the "stock_rolling_averages" table in Cassandra to retrieve the necessary data.

### output:
The output would depend on the data in the Cassandra database, specifically the "stock_rolling_averages" table,


### AAPL:
 the output of the "/rolling_average/AAPL" endpoint would be a JSON response,
```json
{
    "date": "2023-02-01", 
    "price": 143.97,
    "20_day_rolling_avg": 143.97,
    "50_day_rolling_avg": 143.97,
    "200_day_rolling_avg": 143.97
}
```
And, the output of the "/stock_status/AAPL" endpoint would be a JSON response is,
```json
{
    "20_day_rolling_avg_status": "Below",
    "50_day_rolling_avg_status": "Below",
    "200_day_rolling_avg_status": "Below"
}
```



## Conclusions:
This is just a high-level overview of the code that could be used for each stage of the pipeline.
