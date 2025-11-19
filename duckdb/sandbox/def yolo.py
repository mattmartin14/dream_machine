import duckdb
import random

def yolo_delete(table_name: str, how_many_rows: int):
    cn = duckdb.connect('important.db')

    for i in range(0, how_many_rows):
        row_id = random.randint(1, 10000)
        cn.execute(f"DELETE FROM {table_name} WHERE id={row_id};")

    cn.close()