package com.datastax.spark.connector.writer

import java.util.concurrent.TimeUnit

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class BasicWriteBenchmark extends SparkTemplate {

  import BasicWriteBenchmark._

  sparkConf
    .setAppName("Write performance test")
    .set("spark.cassandra.output.concurrent.writes", "16")
    .set("spark.cassandra.output.batch.size.bytes", "4096")
    .set("spark.cassandra.output.batch.grouping.key", "partition")

  lazy val conn = CassandraConnector(sparkConf)

  @Setup(Level.Trial)
  def init(): Unit = {
    conn.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS benchmarks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS benchmarks.write_benchmark (p INT, c INT, value VARCHAR, PRIMARY KEY(p, c))")
    }
    sc
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    conn.openSession().getCluster.close()
    sc.stop()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(totalRowsToWrite)
  @Fork(jvmArgs = Array("-Xmx2048m"))
  def writeBenchmark(): Unit = {
    val col =
      for (i <- 1 to cassandraPartitionsToWrite; j <- 1 to rowsPerCassandraPartition)
        yield (i, j, "data:" + i + ":" + j)
    val rdd = sc.parallelize(col, cassandraPartitionsToWrite * rowsPerCassandraPartition / rowsPerSparkPartition)
    rdd.saveToCassandra("benchmarks", "write_benchmark")
  }

  sc.stop()
}

object BasicWriteBenchmark {
  final val cassandraPartitionsToWrite = 100000
  final val rowsPerCassandraPartition = 10
  final val rowsPerSparkPartition = 50000
  final val totalRowsToWrite = cassandraPartitionsToWrite * rowsPerCassandraPartition
}
