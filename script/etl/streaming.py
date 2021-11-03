from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lag, datediff, when
from pyspark.sql.window import Window
from config.initialize import Initialize
from config.config import Config
from pyspark.sql.types import StructType
from . import batch


def init_config():
    initialize_config = Config()
    return initialize_config


def spark_session():
    initialize = Initialize()
    return initialize.start_spark()


def load_csv_datas():
    userSchema = (
        StructType()
        .add("transactionId", "string")
        .add("custId", "integer")
        .add("transactionDate", "string")
        .add("productSold", "string")
        .add("unitsSold", "integer")
    )

    df = (
        spark_session()
        .readStream.option("sep", "|")
        .option("header", "true")
        .option("inferSchema", "true")
        .schema(userSchema)
        .csv(init_config().get_dir_data_sources())
    )
    return df


def main():
    df = load_csv_datas()

    # get the total greates items that sold by userid
    greates_items = batch.greatest_total_units_sold(df)

    # get date transaction date every greatest items
    # greates_items = batch.get_greatest_total_units_sold_date_trx(greates_items, df)

    # get greates_items that the result from this , is a final result of dataframe
    # longest_streak = batch.get_longest_streak(greates_items)

    greates_items.writeStream.format("console").outputMode(
        "update"
    ).start().awaitTermination()
