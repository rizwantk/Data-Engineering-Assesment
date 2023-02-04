# Data-Engineering-Assesment
##Calculation of required data and using SQL and System Design

Here's the table schema for the MySQL to store the CSV data:

## Design an RDBMS table schema to store the CSV data

```sh
use stock;
```
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
```sh
SELECT 
  CONCAT(YEAR(date), '-Q', QUARTER(date)) AS quarter, 
  AVG(high) AS avg_high, 
  AVG(low) AS avg_low, 
  AVG(volume) AS avg_volume 
FROM stock_data 
GROUP BY YEAR(date), QUARTER(date);

```
**output(Limit 10 only):**

## -- Yearly average
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
