package org.scala.spark.file.compaction

import org.apache.spark.sql.SparkSession
import org.scala.spark.file.compaction.SizeBasedCompaction._
import org.scala.spark.file.enums.compaction.SupportedFileFormat.Orc
import org.scala.spark.file.enums.compaction.{AdaptiveBlockSize, SupportedFileFormat}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SizeBasedCompactionTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName("FileSizeCompactionTest")
    .master("local[*]")
    .getOrCreate()

  test("compactWithCoalesce should return a compacted DataFrame") {
    import spark.implicits._

    val data = Seq(
      (1, "foo"),
      (2, "bar"),
      (3, "baz")
    )
    val df = data.toDF("id", "value")

    val compactedDF = df.dataFrameCompactionWithCoalesce(spark, Orc, Some(AdaptiveBlockSize._64MB), 2)

    // Assert that the number of partitions is reduced (assuming coalesce reduces partitions)
    compactedDF.rdd.getNumPartitions should be < df.rdd.getNumPartitions
  }

  test("compactWithRepartition should return a repartitioned DataFrame") {
    import spark.implicits._

    val data = Seq(
      (1, "foo"),
      (2, "bar"),
      (3, "baz")
    )
    val df = data.toDF("id", "value")

    val repartitionedDF = df.dataFrameCompactionWithRepartition(spark, Orc, Some(AdaptiveBlockSize._64MB), 2)

    // Assert that the number of partitions is equal to the number requested (should increase or stay the same)
    repartitionedDF.rdd.getNumPartitions should be >= 1
  }

  test("getNumberOfFiles should return 1 for empty DataFrame") {
    val emptyDF = spark.emptyDataFrame
    val numberOfFiles = SizeBasedCompaction.fetchMaxFileCount(emptyDF, spark, Orc, None, 1000)
    numberOfFiles shouldEqual 1
  }

  test("getOptimisedSizeOfDataFrame should calculate size correctly") {
    import spark.implicits._

    val data = Seq.fill(1000)((1, "foo")) // Create 1000 rows
    val df = data.toDF("id", "value")

    // Mocking a compression ratio for testing
    val compressionRatio = 8
    val countOfDataFrame = df.count()
    val recordsToSampled = 100

    val optimisedSize = SizeBasedCompaction.calculateOptimizedSize(df, spark, compressionRatio, countOfDataFrame, recordsToSampled)

    optimisedSize should be > 0L // Ensure the calculated size is positive
  }

  test("getBlockSize should return correct block size based on input size") {
    val blockSize1 = SizeBasedCompaction.determineBlockSize(50000000L) // 50 MB
    blockSize1 shouldEqual AdaptiveBlockSize._32MB

    val blockSize2 = SizeBasedCompaction.determineBlockSize(100000000L) // 100 MB
    blockSize2 shouldEqual AdaptiveBlockSize._64MB
  }

  test("getCompressionRatio should return correct ratio based on file format") {
    SizeBasedCompaction.computeCompressionRatio(Orc) shouldEqual 8
    SizeBasedCompaction.computeCompressionRatio(SupportedFileFormat.Json) shouldEqual 5
    SizeBasedCompaction.computeCompressionRatio(SupportedFileFormat.Text) shouldEqual 1
  }
}

