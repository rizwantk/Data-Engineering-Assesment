# Data-Engineering-Assesment
Calculation of required data and using SQL and System Design

Here's the table schema for the MySQL to store the CSV data:

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

