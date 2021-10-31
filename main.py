from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lag, datediff, when
from pyspark.sql.window import Window
from config.initialize import Initialize
from config.config import Config


def init_config():
    initialize_config = Config()
    return initialize_config


def spark_session():
    initialize = Initialize()
    return initialize.start_spark()


def load_csv_datas():
    df = spark_session().read.csv(
        init_config().get_data_sources(), sep="|", header=True, inferSchema=True
    )
    return df


def greatest_total_units_sold(df):

    return (
        df.withColumnRenamed("custId", "customer_id")
        .select("customer_id", "productSold", "unitsSold")
        .groupBy("customer_id", "productSold")
        .sum("unitsSold")
        .withColumnRenamed("sum(unitsSold)", "total_sold")
        .withColumn(
            "row",
            row_number().over(
                Window.partitionBy("customer_id").orderBy(col("total_sold").desc())
            ),
        )
        .filter("row = 1")
        .drop(col("row"))
        .orderBy(col("total_sold").desc())
        .drop(col("total_sold"))
        .withColumnRenamed("productSold", "favourite_product")
    )


def get_greatest_total_units_sold_date_trx(greatest_total_units_sold, mainDatas):

    return greatest_total_units_sold.join(
        mainDatas,
        [
            greatest_total_units_sold.customer_id == mainDatas.custId,
            greatest_total_units_sold.favourite_product == mainDatas.productSold,
        ],
        how="inner",
    ).select("customer_id", "transactionDate", "favourite_product", "unitsSold")


def get_longest_streak(get_date_trx_every_items):

    return (
        get_date_trx_every_items.select(
            "customer_id", "favourite_product", "transactionDate"
        )
        .withColumn(
            "lag",
            lag("transactionDate", 1).over(
                Window.partitionBy("customer_id", "favourite_product").orderBy(
                    col("transactionDate")
                )
            ),
        )
        .withColumn("different_day", datediff(col("transactionDate"), col("lag")))
        .where(col("different_day").isNotNull())
        .filter("different_day = 1")
        .withColumn(
            "lag2",
            lag("transactionDate", 1).over(
                Window.partitionBy("customer_id", "favourite_product").orderBy(
                    col("transactionDate")
                )
            ),
        )
        .withColumn(
            "different_day_2",
            when(col("lag2").isNull(), 1).otherwise(
                datediff(col("transactionDate"), col("lag2"))
            ),
        )
        .filter("different_day_2 = 1")
        .drop("transactionDate")
        .drop("lag")
        .drop("different_day")
        .drop("lag2")
        .groupBy("customer_id", "favourite_product")
        .sum("different_day_2")
        .withColumnRenamed("sum(different_day_2)", "longest_streak")
    )


def write_to_rdbms(df, rdbms_type):

    df.write.format("jdbc").mode("overwrite").option(
        "url",
        "jdbc:"
        + rdbms_type
        + "://"
        + init_config().get_postgres_host
        + ":"
        + init_config().get_postgres_port
        + "/"
        + init_config().get_database,
    ).option("dbtable", init_config().table).option(
        "user", init_config().get_postgres_user
    ).option(
        "password", init_config().get_postgres_password
    ).save()


if __name__ == "__main__":

    # load dataframe from csv
    df = load_csv_datas()

    # get the total greates items that sold by userid
    greates_items = greatest_total_units_sold(df)

    # get date transaction date every greatest items
    greates_items = get_greatest_total_units_sold_date_trx(greates_items, df)

    # get greates_items that the result from this , is a final result of dataframe
    longest_streak = get_longest_streak(greates_items)

    longest_streak.show()
