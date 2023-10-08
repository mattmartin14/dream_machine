from google.cloud import bigquery
import os

def stream_insert():

    project_id = os.getenv("GBQ_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    #config the payload here
    orders = []

    order_1 = {"order_id":3, "order_date": "2023-06-05", "order_lines":
                [
                    {"line_nbr":1, "item_id":833},
                    {"line_nbr":2,"item_id":921}
                ]
            }
    
    orders.append(order_1)

    order_2 = {"order_id":4, "order_date": "2022-01-12", "order_lines":
                [
                    {"line_nbr":1, "item_id":552},
                    {"line_nbr":2,"item_id":389},
                    {"line_nbr":3,"item_id":246}
                ]
            }
    
    orders.append(order_2)

    table_id = "{0}.ds1_test.orders_test".format(project_id)

    errs = client.insert_rows_json(table_id, orders)
    if errs != []:
        print('errors while attempting insert: {0}'.format(errs))
    else:
        print('orders loaded to target')

if __name__ == "__main__":
    stream_insert()