package com.datastax.spark.connector.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class SpanningIteratorBenchmark {

  val iterator = Iterator.from(0)
  val groupsOf1 = new SpanningIterator(iterator, (i: Int) => i)
  val groupsOf10 = new SpanningIterator(iterator, (i: Int) => i / 10)
  val groupsOf1000 = new SpanningIterator(iterator, (i: Int) => i / 1000)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(1000000)
  def benchmark_1m_groups_of_1(): Unit = {
    groupsOf10.drop(1000000)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(100000)
  def benchmark_100k_groups_of_10(): Unit = {
    groupsOf10.drop(100000)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(1000)
  def benchmark_1k_groups_of_1k(): Unit = {
    groupsOf1000.drop(1000)
  }

}
