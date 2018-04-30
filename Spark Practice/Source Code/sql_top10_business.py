from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import Row

if __name__=="__main__":
    #cite: http://spark.apache.org/docs/latest/sql-programming-guide.html#tab_python_0
    spark = SparkSession \
        .builder \
        .appName("sql_top10_business") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # Load a text file and convert each line to a Row.
    sc = spark.sparkContext
    review_rdd = sc.textFile("review.csv").map(lambda r: r.split("::"))
    business_rdd = sc.textFile("business.csv").map(lambda b: b.split("::"))

    reviewMap_rdd=review_rdd.map(lambda x: Row(business_id=x[2], user_id=x[1]))

    # Infer the schema, and register the DataFrame as a table.
    schema_review=spark.createDataFrame(reviewMap_rdd) \
                       .distinct()\
                       .groupBy('business_id')\
                       .count()
    #select first 10 business_id
    top10_schema = schema_review.sort("count", ascending=False)\
                         .head(10)
    #?????
    top10_schema = spark.createDataFrame(sc.parallelize(top10_schema))

    businessMap_rdd = business_rdd.map(lambda x: Row(business_id=x[0], full_address=x[1], categories=x[2]))
    # Infer the schema, and register the DataFrame as a table.
    schema_business = spark.createDataFrame(businessMap_rdd)
    #join two table
    join = top10_schema.join(schema_business, top10_schema.business_id == schema_business.business_id, 'inner')\
                        .select(schema_business.business_id, schema_business.full_address, schema_business.categories, 'count').distinct()
    join.repartition(1).write.save('./sql_top10_business', 'csv', 'overwrite')

