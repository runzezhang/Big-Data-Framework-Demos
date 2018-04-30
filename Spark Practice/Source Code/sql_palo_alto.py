from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import Row
from pyspark.sql.types import *

if __name__=="__main__":
    #cite: http://spark.apache.org/docs/latest/sql-programming-guide.html#tab_python_0
    spark = SparkSession \
        .builder \
        .appName("sql_palo_alto") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # Load a text file and convert each line to a Row.
    sc = spark.sparkContext
    review_rdd = sc.textFile("review.csv").map(lambda r: r.split("::"))
    business_rdd = sc.textFile("business.csv").map(lambda b: b.split("::"))

    #print(business_rdd.take(5))
    '''
    When a dictionary of kwargs cannot be defined ahead of time 
    (for example, the structure of records is encoded in a string, 
    or a text dataset will be parsed and fields will be projected differently for different users), 
    a DataFrame can be created programmatically with three steps.

    1.Create an RDD of tuples or lists from the original RDD;
    2.Create the schema represented by a StructType matching the structure of tuples or lists in the RDD created in the step 1.
    3.Apply the schema to the RDD via createDataFrame method provided by SparkSession.

    '''
    # The schema is encoded in a string.
    schemaString_review = "review_id user_id business_id stars"
    schemaString_business = "business_id full_address categories"
    review_fields = [StructField(review_field_name, StringType(), True) for review_field_name in schemaString_review.split()]
    business_fields = [StructField(business_field_name, StringType(), True) for business_field_name in schemaString_business.split()]
    schema_review = StructType(review_fields)
    schema_business = StructType(business_fields)

    # Apply the schema to the RDD.
    review_scheme = spark.createDataFrame(review_rdd, schema_review)

    review_frame = review_scheme.select("business_id", "user_id", "stars")

    business_scheme = spark.createDataFrame(business_rdd, schema_business)

    review_scheme.limit(5).show()

    #address include Palo Alto
    business_frame = business_scheme.filter(business_scheme.full_address.like('%Palo Alto%'))

    business_frame.limit(5).show()


    # join review and business table
    join = business_frame.join(review_frame, business_frame.business_id == review_frame.business_id, 'inner')
    join.select('user_id',join.stars.cast('int').alias('stars')).groupBy('user_id').avg("stars")\
        .repartition(1).write.save('./sql_palo_alto', 'csv', 'overwrite')






