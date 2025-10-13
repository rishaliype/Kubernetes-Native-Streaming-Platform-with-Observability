package com.citystream.consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DataAnalyzer {

    public static void main(String[] args) {
        String dataPath = args.length > 0 ? args[0] : "/opt/spark-output";
        
        SparkSession spark = SparkSession.builder()
            .appName("CityStream-Data-Analysis")
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== CityStream Data Analysis Dashboard ===\n");

        // Load high-severity alerts
        Dataset<Row> alerts = spark.read()
            .json(dataPath + "/high-severity-alerts");

        // Load windowed aggregations (if any completed windows exist)
        Dataset<Row> windowedAgg = null;
        try {
            windowedAgg = spark.read()
                .json(dataPath + "/windowed-aggregations");
        } catch (Exception e) {
            System.out.println("No windowed aggregations data yet (windows still open)\n");
        }

        // Query 1: Event count by city and severity
        System.out.println("=== Query 1: Events by City and Severity ===");
        alerts.groupBy("city", "severity")
            .count()
            .orderBy(desc("count"))
            .show(20, false);

        // Query 2: Top cities with most high-severity events
        System.out.println("=== Query 2: Top Cities - High Severity Events ===");
        alerts.groupBy("city")
            .count()
            .orderBy(desc("count"))
            .show(10, false);

        // Query 3: Events by type and severity
        System.out.println("=== Query 3: Event Type Distribution ===");
        alerts.groupBy("event_type", "severity")
            .count()
            .orderBy(col("event_type"), desc("count"))
            .show(20, false);

        // Query 4: Hourly pattern analysis
        System.out.println("=== Query 4: Hourly Event Pattern ===");
        alerts.withColumn("hour", hour(col("event_time")))
            .groupBy("hour")
            .count()
            .orderBy("hour")
            .show(24, false);

        // Query 5: Processing lag analysis
        System.out.println("=== Query 5: Processing Lag Statistics ===");
        Dataset<Row> lagAnalysis = alerts
            .withColumn("lag_seconds", 
                unix_timestamp(col("processing_time")).minus(unix_timestamp(col("event_time"))))
            .select(
                avg("lag_seconds").alias("avg_lag"),
                min("lag_seconds").alias("min_lag"),
                max("lag_seconds").alias("max_lag"),
                stddev("lag_seconds").alias("stddev_lag")
            );
        lagAnalysis.show(false);

        // Query 6: Recent critical events
        System.out.println("=== Query 6: Most Recent Critical Events ===");
        alerts.filter(col("severity").equalTo("critical"))
            .orderBy(desc("event_time"))
            .select("event_time", "city", "event_type", "description")
            .show(10, false);

        // Query 7: If windowed aggregations exist, analyze patterns
        if (windowedAgg != null && windowedAgg.count() > 0) {
            System.out.println("=== Query 7: Time Window Analysis ===");
            windowedAgg
                .select("window_start", "window_end", "city", "event_type", "event_count")
                .orderBy(desc("event_count"))
                .show(20, false);

            System.out.println("=== Query 8: Peak Activity Windows ===");
            windowedAgg
                .groupBy("window_start", "window_end")
                .agg(sum("event_count").alias("total_events"))
                .orderBy(desc("total_events"))
                .show(10, false);
        }

        // Query 9: City-specific severity breakdown
        System.out.println("=== Query 9: City-Specific Severity Breakdown ===");
        alerts.groupBy("city")
            .pivot("severity")
            .count()
            .na().fill(0)
            .show(10, false);

        System.out.println("\n=== Analysis Complete ===");
        
        spark.stop();
    }
}