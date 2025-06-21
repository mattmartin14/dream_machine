from faker import Faker
from pyspark.sql.types import StringType

def create_fake_email():
    f = Faker()
    return f.email()

spark.udf.register("get_email", create_fake_email, StringType())