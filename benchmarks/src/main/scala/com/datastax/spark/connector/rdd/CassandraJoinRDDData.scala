package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import com.datastax.driver.core.{ResultSetFuture, Session}
import com.datastax.spark.connector.SparkTemplate
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraJoinRDDBenchmark._

object CassandraJoinRDDData extends App with SparkTemplate {

  val someContent =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam quis augue
      tristique, ultricies nisl sit amet, condimentum felis. Ut blandit nisi eget imperdiet pretium. Ut a
      erat in tortor ultrices posuere quis eu dui. Lorem ipsum dolor sit amet, consectetur adipiscing
      elit. Integer vestibulum vitae arcu ac vehicula. Praesent non erat quis ipsum tempor tempus
      vitae quis neque. Aenean eget urna egestas, lobortis velit sed, vestibulum justo. Nam nibh
      risus, bibendum non ex ac, bibendum varius purus. """

  def createKeyspaceCql(name: String) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createTableCql(keyspace: String, tableName: String): String =
    s"""
       |CREATE TABLE $keyspace.$tableName (
       |  key INT,
       |  group BIGINT,
       |  value TEXT,
       |  PRIMARY KEY (key, group)
       |)""".stripMargin

  def insertData(session: Session, keyspace: String, table: String, partitions: Int,
    partitionSize: Int): Unit = {
    def executeAsync(data: (Int, Long, String)): ResultSetFuture = {
      session.executeAsync(
        s"INSERT INTO $Keyspace.$table (key, group, value) VALUES (?, ?, ?)",
        data._1: Integer, data._2.toLong: JLong, data._3.toString
      )
    }

    val tasks = for (partition <- 0 until partitions; group <- 0 until partitionSize)
      yield executeAsync(partition, group, s"${partition}_${group}_$someContent")

    tasks.par.foreach(_.getUninterruptibly())
  }

  val conn = CassandraConnector(sparkConf)
  conn.withSessionDo { session =>
    session.execute(s"DROP KEYSPACE IF EXISTS $Keyspace")
    session.execute(createKeyspaceCql(Keyspace))

    session.execute(createTableCql(Keyspace, "one_element_partitions"))
    session.execute(createTableCql(Keyspace, "small_partitions"))
    session.execute(createTableCql(Keyspace, "moderate_partitions"))
    session.execute(createTableCql(Keyspace, "big_partitions"))

    insertData(session, Keyspace, "one_element_partitions", Rows, 1)
    insertData(session, Keyspace, "small_partitions", Rows / SmallPartitionSize, SmallPartitionSize)
    insertData(session, Keyspace, "moderate_partitions", Rows/ ModeratePartitionSize, ModeratePartitionSize)
    insertData(session, Keyspace, "big_partitions", Rows / BigPartitionSize, BigPartitionSize)
  }
}
