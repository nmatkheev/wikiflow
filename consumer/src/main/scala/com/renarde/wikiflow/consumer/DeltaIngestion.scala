package com.renarde.wikiflow.consumer

import java.sql.Timestamp

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => f}


object DeltaIngestion extends App with LazyLogging {

  val appName: String = "delta-ingestion"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  logger.info("Initializing delta consumer")

  // Define schema of stream

  val inputStream = spark.readStream
    .format("delta")
//    .option("ignoreChanges", "true")
    .load("/storage/analytics-consumer/output")

  /*
  Advanced:
- попробуйте написать consumer-читатель из папки в которую пишет AnalyticsConsumer, который просто будет выводить его на консоль.
- попробуйте добавить sliding-window который агрегирует подсчет
   */
  val preStream = inputStream
    .select('type.cast(StringType), 'counter.cast(IntegerType), 'xtimestamp.cast(TimestampType))
    .as[(String, String, Timestamp)]

  val transformedStream = preStream
    .withWatermark("xtimestamp", "10 minutes")
    .groupBy(f.window('xtimestamp, "2 minutes", "30 seconds"),
      'type
    )
    .count()

  val consoleOutput = transformedStream.writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
