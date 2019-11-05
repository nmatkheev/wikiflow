package com.renarde.wikiflow.consumer

import java.sql.Timestamp

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  logger.info("Initializing Structured consumer")

  // Define schema of stream
  val expectedSchema = new StructType()
    .add(StructField("bot", BooleanType))
    .add(StructField("comment", StringType))
    .add(StructField("id", LongType))
    .add("length", new StructType()
      .add(StructField("new", LongType))
      .add(StructField("old", LongType))
    )
    .add("meta", new StructType()
      .add(StructField("domain", StringType))
      .add(StructField("dt", StringType))
      .add(StructField("id", StringType))
      .add(StructField("offset", LongType))
      .add(StructField("partition", LongType))
      .add(StructField("request_id", StringType))
      .add(StructField("stream", StringType))
      .add(StructField("topic", StringType))
      .add(StructField("uri", StringType))
    )
    .add("minor", BooleanType)
    .add("namespace", LongType)
    .add("parsedcomment", StringType)
    .add("patrolled", BooleanType)
    .add("revision", new StructType()
      .add("new", LongType)
      .add("old", LongType)
    )
    .add("server_name", StringType)
    .add("server_script_path", StringType)
    .add("server_url", StringType)
    .add("timestamp", LongType)
    .add("title", StringType)
    .add("type", StringType)
    .add("user", StringType)
    .add("wiki", StringType)
  // -----------------------------------------

  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

  /*
  inputStream schema:
  key:           BinaryType
  value:         BinaryType
  topic:         StringType
  partition:     IntegerType
  offset:        LongType
  timestamp:     TimestampType
  timestampType: IntegerType
   */

  // please edit the code below

  /*
  сгруппировать по полю "type", посчитать каунты, добавить текущий timestamp
   */
  val preStream = inputStream
    .select('key.cast(StringType), 'value.cast(StringType), 'timestamp.as("systamp"))
    .as[(String, String, Timestamp)]

  val pre2Stream = preStream
    .filter('value.isNotNull)
    .select(from_json('value, expectedSchema).as("p_data"), 'systamp)
    .select("p_data.*", "systamp")
//    .withColumnRenamed("timestamp", "int_ts")
//    .withColumn("timestamp", ('int_ts / 1000).cast(TimestampType))
//    .drop("int_ts")
    .filter('bot =!= true)

  pre2Stream.schema.printTreeString()

  /*
  Нифига не работает -  append + при группировке всегда нужно окно?
    * val transformedStream = pre2Stream.groupBy("type").count()
    * val transformedStream = pre2Stream.withWatermark("spark_timestamp", "10 minutes").groupBy("type").count()
   */

  val transformedStream = pre2Stream
    .withWatermark("systamp", "10 minutes")
    .groupBy(
      f.window('systamp, "2 minutes", "30 seconds"),
      'type
    )
    .agg(f.count('id) as "counter")
    .select('type, 'counter)
    .withColumn("xtimestamp", f.current_timestamp())

  transformedStream.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start("/storage/analytics-consumer/output")


  spark.streams.awaitAnyTermination()
}
