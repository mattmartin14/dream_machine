##### Author: Matt Martin
##### Date: Circa 2024
This repo is just a scratch pad for some basic spark experimentation. To start a spark session, you can use this boiler plate python code:

```python
import os
dw_path = os.path.expanduser("~")+'/test_dummy_data/spark/test_dw'

## create the spark connection/instance
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test") \
    .config("spark.sql.warehouse.dir", dw_path) \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.instances", 10) \
    .config("spark.jars.packages", "io.dataflint:spark_2.12:0.1.4") \
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin") \
    .getOrCreate()
```

This code limits the amount of memory engaged to avoid an out-of-memory exception. It also loads in the dataflint UI exention so that the spark UI is easier to understand. To see the spark UI, go to  [http://localhost:4040](http://localhost:4040).