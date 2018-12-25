package com.example.etl;

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}

case class LogRecord(appId: String, views: Int) {}

case class ScoreRecord(id: String, score: Int) {}

object ScoreJoiner {

    // first arg is path to app logs
    // second arg is path to scores
    // third arg is output
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("ScoreJoiner").getOrCreate();
        import spark.implicits._
        val logSchema = (new StructType).add("appId", IntegerType).add("views", IntegerType)
        val appLogs = spark.read.schema(logSchema).json(args(0)).as[LogRecord]
        val scoreSchema = (new StructType).add("id", IntegerType).add("score", IntegerType)
        val scores = spark.read.schema(scoreSchema).json(args(1)).as[ScoreRecord]
        val distinct_app_ids = appLogs.select("appId").distinct()
        val joined = distinct_app_ids.join(scores, distinct_app_ids("appId") === scores("id")).
            select("appId","score","views").write.json(args(2))
    }
}