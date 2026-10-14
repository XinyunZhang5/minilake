package io.minilake.spark.bench

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

object Benchmark extends Tag("io.minilake.spark.Benchmark")

object BenchSupport {
  def session(name: String, extra: (String, String)*): SparkSession = {
    val b = SparkSession.builder()
      .master("local[8]")
      .appName(name)
      .config("spark.ui.enabled", "false")
    extra.foreach { case (k, v) => b.config(k, v) }
    b.getOrCreate()
  }

  def tempDir(): Path = Files.createTempDirectory("minilake-bench")

  /**
   * Median wall-clock millis of `runs` timed runs after one warm-up. `body` must build its
   * DataFrame afresh each time, or later runs reuse the first run's shuffle output.
   */
  def medianMillis(runs: Int)(body: => Any): Double = {
    body
    val times = (1 to runs).map { _ =>
      val t0 = System.nanoTime()
      body
      (System.nanoTime() - t0) / 1e6
    }.sorted
    times(times.size / 2)
  }

  /** Appends a section to target/benchmarks/<file> and echoes it. */
  def report(file: String, markdown: String): Unit = {
    val out = Path.of("target", "benchmarks", file)
    Files.createDirectories(out.getParent)
    Files.writeString(out, markdown + "\n", java.nio.file.StandardOpenOption.CREATE,
      java.nio.file.StandardOpenOption.APPEND)
    println(markdown)
  }

  def table(header: Seq[String], rows: Seq[Seq[Any]]): String =
    (Seq(header.mkString("| ", " | ", " |"), header.map(_ => "---").mkString("|", "|", "|")) ++
      rows.map(_.map(_.toString).mkString("| ", " | ", " |"))).mkString("\n")

  case class TaskSample(stageId: Int, millis: Long, shuffleRecordsRead: Long)

  /** Collects per-task duration and shuffle-read records for everything run inside `body`. */
  def withTasks[T](spark: SparkSession)(body: => T): (T, Seq[TaskSample]) = {
    val samples = new ConcurrentLinkedQueue[TaskSample]()
    val listener = new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskMetrics != null) {
        samples.add(TaskSample(e.stageId, e.taskInfo.duration, e.taskMetrics.shuffleReadMetrics.recordsRead))
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val out = body
      // Task-end events are delivered asynchronously; wait until the bus has drained.
      org.apache.spark.ListenerBusAccess.drain(spark.sparkContext)
      (out, samples.asScala.toSeq)
    } finally spark.sparkContext.removeSparkListener(listener)
  }

  def middle(xs: Seq[Long]): Long = if (xs.isEmpty) 0 else xs.sorted.apply(xs.size / 2)
}
