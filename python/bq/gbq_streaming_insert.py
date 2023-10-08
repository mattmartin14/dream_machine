from google.cloud import bigquery
import os

def stream_insert():

    project_id = os.getenv("GBQ_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    #config the payload here
    payload = {"order_id":3, "order_date": "2023-06-05", "order_lines":
                [
                    {"line_nbr":1, "item_id":833},
                    {"line_nbr":2,"item_id":921}
                ]
            }

    table_id = "{0}.ds1_test.orders_test".format(project_id)

    errs = client.insert_rows_json(table_id, [payload])
    if errs != []:
        print('errors while attempting insert: {0}'.format(errs))

if __name__ == "__main__":
    stream_insert()