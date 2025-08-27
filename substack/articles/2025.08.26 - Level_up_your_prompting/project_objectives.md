### Project Overview

For this project, we want to build a basic SQL parser in python. The parser will be able to take in a sql string, parse the tables and columns used, and output the results in a json string.


### Example Implementation

What I'd like to see is a function that can take in a sql string like the following example:

```sql

select a.order_id, a.order_dt, b.sku_nbr, b.cost_amt, b.quantity
from orders as a
    join order_details as b
        on a.order_id = b.order_id

```

And output a JSON string as follows:

```json
{
    "tables": [
        {
            "table_name": "orders",
            "alias" : "a",
            "columns" : ["order_id", "order_dt"]
        },
        {
            "table_name": "order_details",
            "alias" : "b",
            "columns" : ["sku_nbr", "cost_amt", "quantity"]
        }
    ]

}

```

### Hard Requirements
1. I do not want to use existing libaries such as sqlglot or others; i want this built just using standard python libs only
2. Any supplemental files you need to make during the process to debug should get stashed in the "misc" folder so it does not pollute the main src and test folders
3. Use a python virtual environment for this work; i don't want you installing packages or doing stuff in the main python environment

### Deliverables
1. A sql_parser.py file that contains the main function
2. a python notebook that demonstrates the usage of importing this function and running example sql against it; have the outputted json formatted in pretty print so its easy for the eye to see what happened
3. a sql_parser_test.py file that contains at least half a dozen test cases that we can run and ensure the parser works correctly.


### Addtional notes
I'm on python3 so when you issue commands in terminal, be aware. 