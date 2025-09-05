import duckdb
def main():
    
    cn = duckdb.connect("c2_data.db")
    cn.sql("create or replace table summary_workouts as select * from read_csv_auto('~/concept2/workouts/*summary*.csv')")   
    cn.sql("create or replace view v_summary_workouts as select * from read_csv_auto('~/concept2/workouts/*summary*.csv')")   

if __name__ == "__main__":
    main()
