import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from etl_load import ETLLoad
from analytical_transformations import AnalyticalETL


def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.\
            appName("Spring Capital Analytical ETL").\
            getOrCreate()

class ETLLoad:
    """
    This class performs transformation operations on the dataset.
    Transform timestamp format, clean text part, remove extra spaces etc
    """
    def __init__(self,spark):
        self.spark=spark
        self._load_path = config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = config.get('BUCKET', 'PROCESSED_ZONE')


    # End of load of trade records
    def etl_load_trade(self):
        logging.debug("Inside transform parking occupancy dataset module")

        trade_common = self.spark.read.\
                       parquet(self._load_path+"/partition=T/*.parquet")

        trade = trade_common.select("trade_dt", "symbol", "exchange",\
                             "event_tm","event_seq_nb", "arrival_tm", "trade_pr")




        trade_corrected=trade.withColumn("row_number",F.row_number().over(Window.partitionBy(trade.trade_dt,\
                        trade.symbol,trade.exchange,trade.event_tm,trade.event_seq_nb) \
                        .orderBy(trade.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")



        trade_corrected.show(3,truncate=False)

        logging.debug("Writting transformed trade dataframe to a trade partition")
        trade_corrected.withColumn("trade_date", F.col("trade_dt")).write.\
                       partitionBy("trade_dt").mode("overwrite").parquet(self._save_path+"/trade/")


     # End of load of quote records
    def etl_load_quote(self):
        logging.debug("Inside transform parking occupancy dataset module")

        quote_common = self.spark.read.\
                       parquet(self._load_path+"/partition=Q/*.parquet")

        quote=quote_common.select("trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm", \
                                 "bid_pr","bid_size","ask_pr","ask_size")

        quote_corrected=quote.withColumn("row_number",F.row_number().over(Window.partitionBy(quote.trade_dt,quote.symbol,\
                                            quote.exchange,quote.event_tm,quote.event_seq_nb).\
                                            orderBy(quote.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")


        quote_corrected.show(3,truncate=False)

        quote_corrected.printSchema()

        logging.debug("Writting transformed quote dataframe to a trade partition")
        quote_corrected.withColumn("trade_date", F.col("trade_dt")).write.\
                       partitionBy("trade_dt").mode("overwrite").parquet(self._save_path+"/quote/")



def main():
    """
    Driver code to perform the following steps
    1. Works on the pre-processed parquet files and transforms and loads the end of day trade and quote records
    2. Executes the final analytical_etl function that uses SparkSQL and Python to build an ETL job
        that calculates the following results for a given day:
        - Latest trade price before the quote.
        - Latest 30-minute moving average trade price, before the quote.
        - The bid/ask price movement from previous day’s closing price
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    etl_load = ETLLoad(spark)
    analy_etl = AnalyticalETL(spark)

    # cleaning tables
    logging.debug("\n\nCleaning Hive Tables...")
    spark.sql("DROP TABLE IF EXISTS trade")
    spark.sql("DROP TABLE IF EXISTS temp_trade")
    spark.sql("DROP TABLE IF EXISTS temp_trade_mov_avg")
    spark.sql("DROP TABLE IF EXISTS prev_trade")
    spark.sql("DROP TABLE IF EXISTS prev_temp_last_trade")
    spark.sql("DROP TABLE IF EXISTS temp_quote")
    spark.sql("DROP TABLE IF EXISTS quotes")
    spark.sql("DROP TABLE IF EXISTS quotes_union")
    spark.sql("DROP TABLE IF EXISTS trades_latest")
    spark.sql("DROP TABLE IF EXISTS quotes_update")


    # Modules in the project
    modules = {
         "trade.parquet": etl_load.etl_load_trade,
         "quote.parquet" : etl_load.etl_load_quote,
         "analytical_quote": analy_etl.anaytical_etl
    }

    for file in modules.keys():
        modules[file]()




# Entry point for the pipeline
if __name__ == "__main__":
    main()
