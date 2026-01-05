
import duckdb

def generate_insights() -> None:
    cn = duckdb.connect()

    sql = f"""
    WITH orders AS (
        SELECT order_id, cust_id, order_date, total_amount FROM read_parquet('../data/orders.parquet')
    ),
    insights AS (
        SELECT
            o.order_id,
            o.cust_id,
            -- Survey date shortly after the order date
            (o.order_date + (round(random() * 14))::INT) AS survey_date,
            -- Ratings 5-10 range
            5 + (round(random()*5))::INT AS overall_satisfaction,
            5 + (round(random()*5))::INT AS product_quality,
            5 + (round(random()*5))::INT AS staff_friendliness,
            5 + (round(random()*5))::INT AS store_cleanliness,
            5 + (round(random()*5))::INT AS delivery_experience,
            5 + (round(random()*5))::INT AS value_for_money,
            CASE round(random()*10)::INT
                WHEN 0 THEN 'Detractor'
                WHEN 1 THEN 'Detractor'
                WHEN 2 THEN 'Passive'
                WHEN 3 THEN 'Passive'
                WHEN 4 THEN 'Passive'
                ELSE 'Promoter'
            END AS nps_category,
            round(random()*100)::INT AS recommend_probability
        FROM orders o
    )
    SELECT * FROM insights
    """

    cn.execute(f"COPY ({sql}) TO '../data/customer_insights.parquet'")

if __name__ == "__main__":
    generate_insights()
