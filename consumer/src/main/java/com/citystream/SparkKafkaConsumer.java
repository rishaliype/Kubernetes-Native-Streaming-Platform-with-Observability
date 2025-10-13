package com.citystream.consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkKafkaConsumer {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        
        String kafkaBootstrapServers = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", 
            "kafka:9092"
        );
        String kafkaTopic = "city-events";
        String outputPath = System.getenv().getOrDefault(
            "SPARK_OUTPUT_PATH",
            "/opt/spark-output"  // K8s mounted volume
        );
        
        System.out.println("Starting Enhanced Spark Kafka Consumer...");
        System.out.println("Kafka: " + kafkaBootstrapServers);
        System.out.println("Topic: " + kafkaTopic);
        System.out.println("Output Path: " + outputPath);

        SparkSession spark = SparkSession.builder()
            .appName("CityStream-Enhanced-Consumer")
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Schema with proper timestamp type
        StructType schema = new StructType()
            .add("city", DataTypes.StringType)
            .add("event_type", DataTypes.StringType)
            .add("severity", DataTypes.StringType)
            .add("description", DataTypes.StringType)
            .add("timestamp", DataTypes.StringType);

        Dataset<Row> kafkaStream = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", "earliest")
            .option("kafka.group.id", "citystream-spark-consumer")
            .load();

        // Parse JSON with error handling
        Dataset<Row> parsedEvents = kafkaStream
            .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp")
            .select(
                col("json"),
                col("kafka_timestamp"),
                from_json(col("json"), schema).as("data")
            );

        // Separate valid and invalid events
        Dataset<Row> validEvents = parsedEvents
            .filter(col("data").isNotNull())
            .select("data.*", "kafka_timestamp")
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .withColumn("processing_time", current_timestamp());

        Dataset<Row> invalidEvents = parsedEvents
            .filter(col("data").isNull())
            .select(col("json"), col("kafka_timestamp"));

        // === Query 1: Windowed Aggregations (5-minute windows) ===
        Dataset<Row> windowedCounts = validEvents
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("city"),
                col("event_type")
            )
            .agg(
                count("*").alias("event_count"),
                collect_list("severity").alias("severities"),
                max("processing_time").alias("last_processed")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("city"),
                col("event_type"),
                col("event_count"),
                col("severities"),
                col("last_processed")
            );

        // === Query 2: High-Severity Alert Stream ===
        Dataset<Row> highSeverityEvents = validEvents
            .filter(col("severity").isin("high", "critical"))
            .select(
                col("event_time"),
                col("city"),
                col("event_type"),
                col("severity"),
                col("description"),
                col("processing_time")
            );

        // === Query 3: City Summary (1-minute windows) ===
        Dataset<Row> citySummary = validEvents
            .withWatermark("event_time", "2 minutes")
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("city")
            )
            .agg(
                count("*").alias("total_events"),
                sum(when(col("severity").equalTo("critical"), 1).otherwise(0)).alias("critical_count"),
                sum(when(col("severity").equalTo("high"), 1).otherwise(0)).alias("high_count"),
                sum(when(col("severity").equalTo("medium"), 1).otherwise(0)).alias("medium_count"),
                sum(when(col("severity").equalTo("low"), 1).otherwise(0)).alias("low_count")
            )
            .select(
                col("window.start").alias("minute"),
                col("city"),
                col("total_events"),
                col("critical_count"),
                col("high_count"),
                col("medium_count"),
                col("low_count")
            );

        // ADD LAG TRACKING HERE - after all dataset definitions, before starting queries
        Dataset<Row> eventsWithLag = validEvents
            .withColumn("lag_seconds", 
                unix_timestamp(col("processing_time")).minus(unix_timestamp(col("event_time"))));

        // Start multiple queries
        System.out.println("\n=== Starting Streaming Queries ===\n");

        // Query 1a: Windowed aggregations to console
        StreamingQuery windowConsoleQuery = windowedCounts
            .writeStream()
            .outputMode("update")
            .format("console")
            .option("truncate", false)
            .option("checkpointLocation", outputPath + "/checkpoints/windowed-console")
            .queryName("windowed-console")
            .start();

        // Query 1b: Windowed aggregations to file (PERSISTENT)
        StreamingQuery windowFileQuery = windowedCounts
            .writeStream()
            .outputMode("append")  // Append for files
            .format("json")
            .option("path", outputPath + "/windowed-aggregations")
            .option("checkpointLocation", outputPath + "/checkpoints/windowed-file")
            .queryName("windowed-file")
            .start();

        // Query 2a: High-severity alerts to console
        StreamingQuery alertConsoleQuery = highSeverityEvents
            .writeStream()
            .outputMode("append")
            .format("console")
            .option("truncate", false)
            .option("checkpointLocation", outputPath + "/checkpoints/alerts-console")
            .queryName("alerts-console")
            .start();

        // Query 2b: High-severity alerts to file (PERSISTENT)
        StreamingQuery alertFileQuery = highSeverityEvents
            .writeStream()
            .outputMode("append")
            .format("json")
            .option("path", outputPath + "/high-severity-alerts")
            .option("checkpointLocation", outputPath + "/checkpoints/alerts-file")
            .queryName("alerts-file")
            .start();

        // Query 3: City summary to console only
        StreamingQuery summaryQuery = citySummary
            .writeStream()
            .outputMode("update")
            .format("console")
            .option("truncate", false)
            .option("checkpointLocation", outputPath + "/checkpoints/summary")
            .queryName("city-summary")
            .start();

        // Query 4: Invalid events monitoring
        StreamingQuery errorQuery = invalidEvents
            .writeStream()
            .outputMode("append")
            .format("console")
            .option("checkpointLocation", outputPath + "/checkpoints/errors")
            .queryName("parse-errors")
            .start();
        
        // Query 5: Lag metrics monitoring
        StreamingQuery lagQuery = eventsWithLag
            .groupBy()  // Global aggregation
            .agg(
                avg("lag_seconds").alias("avg_lag_seconds"),
                max("lag_seconds").alias("max_lag_seconds"),
                count("*").alias("events_processed")
            )
            .writeStream()
            .outputMode("complete")
            .format("console")
            .option("checkpointLocation", outputPath + "/checkpoints/lag-metrics")
            .queryName("lag-metrics")
            .start();

        System.out.println("All queries started:");
        System.out.println("- Windowed aggregations: Console + File");
        System.out.println("- High-severity alerts: Console + File");
        System.out.println("- City summary: Console");
        System.out.println("- Parse errors: Console");
        System.out.println("\nProcessing events...\n");
        
        // Wait for all queries
        spark.streams().awaitAnyTermination();
    }
}