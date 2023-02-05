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
  AVG(high) AS avg_high, 
  AVG(low) AS avg_low, 
  AVG(volume) AS avg_volume 
  FROM stock_data 
  GROUP BY DATE_FORMAT(date, '%xW%v');
```
**output(Limit 10 only):**
| Week | Average High | Average Low | Average Volume |
|------|--------------|-------------|---------------|
| 2017W08 | 64.64000193277995 | 64.12666829427083 | 20454200.0000 |
| 2017W09 | 64.55199890136718 | 63.86599960327148 | 21744860.0000 |
| 2017W10 | 64.97599945068359 | 64.29600143432617 | 19633440.0000 |
| 2017W11 | 64.93200073242187 | 64.39000091552734 | 25821600.0000 |
| 2017W12 | 65.30199890136718 | 64.5 | 20760980.0000 |
| 2017W13 | 65.67200164794922 | 64.95199890136719 | 17695320.0000 |
| 2017W14 | 66.0239990234375 | 65.36600189208984 | 17411780.0000 |
| 2017W15 | 65.70000076293945 | 65.0674991607666 | 17937300.0000 |
| 2017W16 | 65.87999877929687 | 65.13000030517578 | 22731960.0000 |
| 2017W17 | 68.30599975585938 | 67.51800079345703 | 32144660.0000 |
| ... | ... | ... | ... |
## -- Monthly average
Use the following SQL query to calculate the Monthly average of High, Low, and Volume:
```SQL
SELECT 
  DATE_FORMAT(date, '%Y-%m') AS month, 
  AVG(high) AS avg_high, 
  AVG(low) AS avg_low, 
  AVG(volume) AS avg_volume 
FROM stock_data 
GROUP BY DATE_FORMAT(date, '%Y-%m');
```
**output(Limit 10 only):**
| Month | Average High | Average Low | Average Volume |
|-------|--------------|-------------|---------------|
| 2017-02 | 64.53200073242188 | 64.0380012512207 | 20094780.0000 |
| 2017-03 | 65.14913044805112 | 64.44391316952913 | 21268247.8261 |
| 2017-04 | 66.51842057077508 | 65.80736903140419 | 22799536.8421 |
| 2017-05 | 69.2095454822887 | 68.44181754372336 | 23509931.8182 |
| 2017-06 | 71.01454509388317 | 69.8354537270286 | 28623490.9091 |
| 2017-07 | 72.41299934387207 | 71.44099960327148 | 23492560.0000 |
| 2017-08 | 73.19608671768852 | 72.28521761686906 | 19307413.0435 |
| 2017-09 | 74.78600006103515 | 73.89099922180176 | 18799195.0000 |
| 2017-10 | 78.34954521872781 | 77.53000051325017 | 20452272.7273 |
| 2017-11 | 84.08809480212983 | 83.15857187906902 | 20091714.2857 |
| ... | ... | ... | ... |

## -- Quarterly average
Use the following SQL query to calculate the Quarterly average of High, Low, and Volume:
```SQL
SELECT 
  extract(year from date) AS year,
  extract(QUARTER from date) AS quarter, 
  AVG(high) AS avg_high, 
  AVG(low) AS avg_low, 
  AVG(volume) AS avg_volume 
FROM stock_data 
GROUP BY extract(year from date),
  extract(QUARTER from date);

```
**output:**
Year | Quarter | avg_high | avg_low | avg_volume
---- | ------- | -------- | ------ | ---------
2017 | 1       | 65.0389  | 64.3714 | 21058700.0000
2017 | 2       | 69.0282  | 68.1339 | 25081373.0159
2017 | 3       | 73.4522  | 72.5269 | 20474692.0635
2017 | 4       | 82.5036  | 81.5120 | 21239353.9683
2018 | 1       | 92.5382  | 90.4490 | 33617647.5410
2018 | 2       | 97.7762  | 95.9659 | 27814590.6250
2018 | 3       | 109.083  | 107.469 | 23908506.3492
2018 | 4       | 108.823  | 105.555 | 41144304.7619
2019 | 1       | 109.821  | 107.980 | 29055811.4754
2019 | 2       | 127.823  | 125.922 | 23630171.4286
2019 | 3       | 138.560  | 136.318 | 24079792.1875
2019 | 4       | 147.661  | 145.941 | 21753101.5625
2020 | 1       | 167.362  | 161.369 | 49334641.9355
2020 | 2       | 183.631  | 178.986 | 38706593.6508
2020 | 3       | 212.650  | 207.192 | 34856109.3750
2020 | 4       | 217.154  | 212.878 | 28122229.6875
2021 | 1       | 234.408  | 229.626 | 30557121.3115
2021 | 2       | 255.783  | 252.124 | 24957439.6825
2021 | 3       | 292.807  | 288.708 | 22918948.4375
2021 | 4       | 326.682  | 320.910 | 25812223.4375
2022 | 1       | 310.669  | 301.809 | 42383070.5882

## -- Yearly average
Use the following SQL query to calculate the Yearly average of High, Low, and Volume:
```SQL
SELECT 
  YEAR(date) AS year, 
  AVG(high) AS avg_high, 
  AVG(low) AS avg_low, 
  AVG(volume) AS avg_volume 
FROM stock_data 
GROUP BY YEAR(date);
```

**output:**
| Year | Average High | Average Low | Average Volume |
|-------|--------------|-------------|---------------|
| 2017 | 73.71009200838854 | 72.80783408028739 | 22109470.0461
| 2018 | 102.11406370368137 | 99.91928313357897 | 31590188.8446
| 2019 | 131.23095252021912 | 129.30384917486282 | 24580994.0476
| 2020 | 195.4651384315943 | 190.37743079144022 | 37659592.4901
| 2021 | 278.0180160280258 | 273.43896805293974 | 26012294.0476
| 2022 | 310.6691158519072 | 301.8094123391544 | 42383070.5882


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
url = "https://finance.yahoo.com/quote/{}/history?p={}"

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
rolling_avg_df = parsed_df \
  .groupBy("stock_symbol") \
  .agg(avg(parsed_df["price"]).over(window(parsed_df["timestamp"], "20 days")).alias("avg_20_days")) \
  .agg(avg(parsed_df["price"]).over(window(parsed_df["timestamp"], "50 days")).alias("avg_50_days")) \
  .agg(avg(parsed_df["price"]).over(window(parsed_df["timestamp"], "200 days")).alias("avg_200_days"))

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
| AAPL | 143.970001 | 01-02-2023 | 143.970001 | 143.970001 | 143.970001 | false | false | false |
| AAPL | 164.699997 | 01-03-2022 | 154.33499899999998 | 154.33499899999998 | 154.33499899999998 | true | true | true |
| AAPL | 174.029999 | 01-04-2022 | 160.89999899999998 | 160.89999899999998 | 160.89999899999998 | true | true | true |
| AAPL | 149.899994 | 01-06-2022 | 158.14999774999998 | 158.14999774999998 | 158.14999774999998 | false | false | false |
| AAPL | 136.039993 | 01-07-2022 | 153.72799679999997 | 153.72799679999997 | 153.72799679999997 | false | false | false |
| AAPL | 161.009995 | 01-08-2022 | 154.94166316666664 | 154.94166316666664 | 154.94166316666664 | true | true | true |
| AAPL | 156.639999 | 01-09-2022 | 155.18428257142855 | 155.18428257142855 | 155.18428257142855 | true | true | true |
| AAPL | 155.080002 | 01-11-2022 | 155.17124749999996 | 155.17124749999996 | 155.17124749999996 | false | false | false |
| AAPL | 148.210007 | 01-12-2022 | 154.3977763333333 | 154.3977763333333 | 154.3977763333333 | false | false | false |

only showing top 10 rows

plot:

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
    query = "SELECT avg_20_days, avg_50_days, avg_200_days FROM stock_rolling_averages WHERE stock_symbol='{}'".format(stock_symbol)
    result = session.execute(query)
    result_dict = {"20_day_rolling_avg": result[0][0], "50_day_rolling_avg": result[0][1], "200_day_rolling_avg": result[0][2]}
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
    "20_day_rolling_avg": 143.970001,
    "50_day_rolling_avg": 143.970001,
    "200_day_rolling_avg": 143.970001
}
```
And, the output of the "/stock_status/AAPL" endpoint would be a JSON response is,
```json
{
    "20_day_rolling_avg_status": "Above",
    "50_day_rolling_avg_status": "Above",
    "200_day_rolling_avg_status": "Above"
}
```



## Conclusions:
This is just a high-level overview of the code that could be used for each stage of the pipeline.
