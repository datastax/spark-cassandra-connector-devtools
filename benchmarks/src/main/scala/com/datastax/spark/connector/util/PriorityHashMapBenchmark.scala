package com.datastax.spark.connector.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import scala.util.Random

/** Benchmarks a simulation of a heap-based stream-sort.
  * In each iteration, a random item is added to the priority queue
  * and the head item is removed. */
@State(Scope.Benchmark)
class PriorityHashMapBenchmark {

  val capacity = 1024 * 8
  val count = 1024 * 8
  val random = new Random

  val m = new PriorityHashMap[Int, Int](capacity)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(1000000)
  def benchmarkingCaller(): Unit = {
    // we need to measure how much the loop and random alone are taking
    for (i <- 1 to 1000000) {
      // 1 million
      random.nextInt(count).asInstanceOf[AnyRef]
      random.nextInt().asInstanceOf[AnyRef]
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(1000000)
  def benchmarkingRealCode(): Unit = {
    for (i <- 1 to 1000000) {
      // 1 million
      if (m.size >= count)
        m.remove(m.headKey)
      m.put(random.nextInt(count), random.nextInt(count))
    }
  }
}
