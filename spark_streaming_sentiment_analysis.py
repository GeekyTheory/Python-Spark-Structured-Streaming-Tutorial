from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, explode, split, udf
from pyspark.sql.types import StringType
from textblob import TextBlob

import settings


def stream_preprocessing(stream_messages):
    return stream_messages.select(
        explode(split(stream_messages.value, settings.ANALYSIS['TWEET_EOL'])).alias('message')) \
        .withColumn('message', regexp_replace('message', r'http\S+', '')) \
        .withColumn('message', regexp_replace('message', '@\w+', '')) \
        .withColumn('message', regexp_replace('message', '#', '')) \
        .withColumn('message', regexp_replace('message', 'RT', '')) \
        .withColumn('message', regexp_replace('message', ':', ''))


def calculate_text_polarity(text) -> float:
    return TextBlob(text).sentiment.polarity


def calculate_text_subjectivity(text) -> float:
    return TextBlob(text).sentiment.subjectivity


def classify_messages(messages):
    polarity_calculation_udf = udf(calculate_text_polarity, StringType())
    subjectivity_calculation_udf = udf(calculate_text_subjectivity, StringType())

    return messages \
        .withColumn("polarity", polarity_calculation_udf("message")) \
        .withColumn("subjectivity", subjectivity_calculation_udf("message"))


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    stream_messages = spark.readStream.format("socket") \
        .option("host", settings.SOCKET['HOST']) \
        .option("port", settings.SOCKET['PORT']) \
        .load()

    classified_messages = classify_messages(stream_preprocessing(stream_messages))

    query = classified_messages.writeStream \
        .format("csv") \
        .option("checkpointLocation", "checkpoint/") \
        .option("path", "analysis_output/") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start() \
        .awaitTermination()
