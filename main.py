"""Entry point for the ETL application

Sample usage:
docker-compose run etl poetry run python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse
  --table transactions
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number,lag,datediff,when
from pyspark.sql.window import Window

spark = SparkSession.builder.master("spark://localhost:7077").appName("etl-apps") \
    .getOrCreate()

df = spark.read.csv('/opt/data/transaction.csv', sep='|', header=True, inferSchema=True)

#################################################################
# ETL Process
greatest_total_units_sold = df.withColumnRenamed("custId","customer_id") \
  .select("customer_id","productSold","unitsSold") \
  .groupBy("customer_id","productSold") \
  .sum("unitsSold").withColumnRenamed("sum(unitsSold)","total_sold") \
  .withColumn("row",row_number().over(Window.partitionBy("customer_id").orderBy(col("total_sold").desc()))) \
  .filter("row = 1") \
  .drop(col("row")) \
  .orderBy(col("total_sold").desc()) \
  .drop(col("total_sold")) \
  .withColumnRenamed("productSold","favourite_product") \
  .withColumnRenamed("","")

get_date_trx_every_items = greatest_total_units_sold.join(df,[
  greatest_total_units_sold.customer_id == df.custId,
  greatest_total_units_sold.favourite_product == df.productSold
  ],how='inner').select("custId","transactionDate","productSold","unitsSold")

get_longest_streak = get_date_trx_every_items.select("custId","productSold","transactionDate") \
  .withColumn("lag",lag("transactionDate",1).over(Window.partitionBy("custId","productSold").orderBy(col("transactionDate")))) \
  .withColumn("different_day",datediff(col("transactionDate"),col("lag"))) \
  .where(col("different_day").isNotNull()) \
  .filter("different_day = 1") \
  .withColumn("lag2",lag("transactionDate",1).over(Window.partitionBy("custId","productSold").orderBy(col("transactionDate")))) \
  .withColumn("different_day_2",
    when(col("lag2").isNull(),1) \
      .otherwise(datediff(col("transactionDate"),col("lag2")))
  ) \
  .filter("different_day_2 = 1") \
  .drop("transactionDate").drop("lag").drop("different_day").drop("lag2") \
  .groupBy("custId","productSold") \
  .sum("different_day_2").withColumnRenamed("sum(different_day_2)","longest_streak") 


get_longest_streak.show()

'''get_longest_streak.write.format('jdbc') \
    .mode("overwrite") \
    .option('url','jdbc:postgresql://localhost:5432/postgres') \
    .option('dbtable','sertis_testing') \
    .option('user','postgres') \
    .option('password','postgres') \
    .save()'''