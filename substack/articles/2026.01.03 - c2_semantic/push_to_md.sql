CREATE OR REPLACE TABLE c2.summary_data2 AS
SELECT *
from read_csv_auto('~/concept2/workouts/*summary*.csv')