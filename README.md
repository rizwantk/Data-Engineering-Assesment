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

### Justification for the architecture:

- Apache Kafka or Apache Flume: These technologies can handle large amounts of data and provide reliable data ingestion.
- Apache Spark Streaming: It provides a real-time data processing engine that can handle high-velocity data.
- Apache Cassandra or Apache HBase: Both of these technologies are optimized for real-time, large-scale data storage and retrieval.
- Flask or Django: These are popular and widely used frameworks for creating REST APIs.

### 1.Data Ingestion:

- Technology: Apache Kafka
- Reason for choosing: Apache Kafka is a highly scalable, distributed, and fault-tolerant message broker that is well-suited for collecting and processing high volumes of real-time data.
- Code Snippet (in Python):
```python
from kafka import KafkaConsumer

# Create Kafka consumer
consumer = KafkaConsumer("stock_trading", bootstrap_servers="localhost:9092")

# Loop over messages in the topic
for msg in consumer:
    # Process the message
    process_message(msg.value)
```
##### How this code works:
- In this code, a Kafka consumer is created to consume messages from the "stock_trading" topic on a Kafka broker running on "localhost:9092".
- The consumer then loops over the messages in the topic and calls the "process_message" function for each message to process it.
- The "process_message" function takes the message as input, converts it from bytes to a string, and parses it into the stock symbol, price, and timestamp.
- The function then calls two other functions: "store_in_database" and "calculate_rolling_averages", to store the stock data in the database and calculate the rolling averages for the stock, respectively.

### 2.Data Processing:

- Technology: Apache Spark Streaming
- Reason for choosing: Apache Spark Streaming is a fast, scalable, and fault-tolerant stream processing engine that supports real-time data processing and batch processing.
- Code Snippet (in Python):

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create Spark context
sc = SparkContext(appName="StockTradingStreaming")

# Create Streaming context
ssc = StreamingContext(sc, batchInterval=1)

# Create DStream from Kafka
dataStream = KafkaUtils.createStream(ssc, "zookeeper_host:2181", "spark_streaming_group", {topic: 1})

# Calculate rolling average
rolling_avg = dataStream.window(windowDuration=200, slideDuration=1).mean()

# Persist the calculated data
rolling_avg.foreachRDD(lambda rdd: rdd.saveAsTextFile("hdfs:///rolling_avg"))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
```
##### How this code works:
 - It first creates SparkContext and StreamingContext.
 - It then creates a DStream from the Kafka topic named 'topic' and specified by the Zookeeper host "zookeeper_host:2181".
 - It calculates the mean value of the stock data for each sliding window of 200 time steps with a sliding interval of 1 time step.
 - It persists the calculated rolling average data to HDFS.
 - Finally, it starts the StreamingContext and awaits termination.

### 3.Data Persistence/Storage:

- Technology: Apache Cassandra
- Reason for choosing: Apache Cassandra is a highly scalable, distributed, and fault-tolerant NoSQL database that supports real-time data processing and real-time data retrieval.
- Code Snippet (in Python):
```python
from cassandra.cluster import Cluster

# Connect to Cassandra cluster
cluster = Cluster(["localhost"])
session = cluster.connect("stock_trading")

# Insert data into Cassandra
session.execute("INSERT INTO rolling_avg (stock_symbol, avg_20, avg_50, avg_200) VALUES (%s, %s, %s, %s)", (stock_symbol, avg_20, avg_50, avg_200))
```
##### How this code works:
- This code connects to a Cassandra database called "stock_trading" running on "localhost" and inserts data into the "rolling_avg" table. 
- The data to be inserted includes the "stock_symbol", and the rolling averages for 20, 50, and 200 days, represented by "avg_20", "avg_50", and "avg_200" respectively. The data is passed as parameters to the "execute" method, which inserts the data into the table.

### 4.API layer:

- Technology: Flask (with Apache Cassandra as the database)
- Reason for choosing: Flask is a lightweight and flexible web framework that provides a simple way to build RESTAPIs. It can easily integrate with Apache Cassandra to retrieve data and make it accessible through API endpoints.
- Code Snippet (in Python):

```python
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/rolling_avg/<stock_symbol>")
def get_rolling_avg(stock_symbol):
    # Retrieve rolling average from database
    avg_20, avg_50, avg_200 = retrieve_rolling_avg(stock_symbol)
    
    # Return the rolling average as JSON
    return jsonify(avg_20=avg_20, avg_50=avg_50, avg_200=avg_200)

if __name__ == "__main__":
    app.run(debug=True)

```
##### How this code works:
This code is a Flask web application that provides an API endpoint to retrieve the rolling average of a stock symbol.
 - The "Flask" module is imported and a new "Flask" application is created.
 - The "retrieve_rolling_avg" function is defined to retrieve the rolling average for a given stock symbol from a database. The implementation details are not provided in the code.
 - A route is defined using the "@app.route" decorator with a URL path of "/rolling_avg/<stock_symbol>". The "get_rolling_avg" function handles this route and calls the "retrieve_rolling_avg" function to retrieve the rolling average for the given stock symbol.
 - The rolling average is returned as a JSON object using the "jsonify" function from the "Flask" module.
 - The " if __name__ == "__main__": " block runs the Flask application in debug mode.
 - This code provides a REST API endpoint that takes a stock symbol as input and returns the rolling average of that stock symbol in JSON format.

### output:
The output of the pipeline would be the rolling average of the trading data for each stock calculated over the past 20 days, 50 days, and 200 days. This data would be persisted in a database of choice (e.g. Apache Cassandra) and could be retrieved by a front-end API (e.g. built using Flask) that would return the rolling averages for a given stock and whether a stock is above or below a rolling average.


### AAPL(rolling average):
the API endpoint is "/rolling_avg/AAPL ", it might return a JSON response :
```json
{
    "avg_20": 123.45,
    "avg_50": 234.56,
    "avg_200": 345.67,
    "is_above_avg_20": true,
    "is_above_avg_50": false,
    "is_above_avg_200": true
}
```
where "avg_20", "avg_50", and "avg_200" are the rolling averages over the past 20 days, 50 days, and 200 days respectively, and "is_above_avg_20", "is_above_avg_50", and "is_above_avg_200" indicate whether the stock is above or below the respective rolling average.


### MSFT(rolling average):
the API endpoint is /rolling_avg/MSFT, it might return a JSON response :
```json
{
    "avg_20": 111.11,
    "avg_50": 222.22,
    "avg_200": 333.33,
    "is_above_avg_20": false,
    "is_above_avg_50": true,
    "is_above_avg_200": false
}
```
where "avg_20", "avg_50", and "avg_200" are the rolling averages over the past 20 days, 50 days, and 200 days respectively, and "is_above_avg_20", "is_above_avg_50", and "is_above_avg_200" indicate whether the stock (MSFT in this case) is above or below the respective rolling average.


## Conclusions:
This is just a high-level overview of the code that could be used for each stage of the pipeline.

 **The input data format of the code is a stock symbol passed as a URL parameter in the API endpoint. For example, if you make a GET request to the endpoint /rolling_avg/AAPL, the stock symbol AAPL is passed to the function get_rolling_avg as an argument.** 

The code then retrieves the rolling averages for the specified stock symbol from the database and returns it as a JSON response. 

 **The output of this code would be a  JSON response  that contains the rolling average of a given stock. The response includes the stock symbol, 20-day rolling average, 50-day rolling average, and 200-day rolling average. The JSON response is returned from the get_rolling_average function when the API endpoint "/rolling_average/<symbol>" is called with a GET request, where <symbol> is the stock symbol you want to retrieve the rolling average for.**
