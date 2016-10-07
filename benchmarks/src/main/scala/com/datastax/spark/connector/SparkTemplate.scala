package com.datastax.spark.connector

import org.apache.spark.{SparkConf, SparkContext}

trait SparkTemplate {

  val sparkConf = SparkTemplate.sparkConf

  lazy val sc = SparkContext.getOrCreate(sparkConf)

}


object SparkTemplate {

  private val _sparkConf = new SparkConf(loadDefaults = true)
    .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    .setIfMissing("spark.cassandra.connection.port", "9042")
    .setIfMissing("spark.cassandra.connection.keep_alive_ms", "5000")
    .setIfMissing("spark.cassandra.connection.timeout_ms", "30000")
    .setIfMissing("spark.ui.showConsoleProgress", "false")
    .setIfMissing("spark.ui.enabled", "false")
    .setIfMissing("spark.cleaner.ttl", "3600")
    .setIfMissing("spark.master", "local[*]")
    .setIfMissing("spark.app.name", "spark-cassandra-connector-benchmark")

  def sparkConf = _sparkConf.clone()

}
