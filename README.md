# Data-Engineering-Assesment
### Stock Market Data Analysis
A project to calculate the weekly,monthly,quarterly,yearly average of High, Low, and Volume from stock market data(MSFT).

### Prerequisites
- A database management system, such as MySQL or PostgreSQL
- Stock market data in a table with columns for date, open, high, low,close,Adj close and volume
- Here we are using the Mysql workbench platform for the following calculation.

### Design an RDBMS table schema to store the CSV data
create a schema and use the schema
```sh
use stock;
```

### SQL Query
Use the following SQL query to create a table:

## --Table creation
```sh
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
```sh
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
```sh
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
```sh
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
```sh
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
