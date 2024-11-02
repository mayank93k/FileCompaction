package org.scala.spark.file.compaction

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scala.spark.file.enums.compaction.AdaptiveBlockSize
import org.scala.spark.file.enums.compaction.AdaptiveBlockSize._
import org.scala.spark.file.enums.compaction.SupportedFileFormat._

/**
 * Optimizes the DataFrame by compacting it into an optimal number of files for writing to the destination.
 */
object SizeBasedCompaction {
  implicit class Compaction(dataFrame: DataFrame) {
    /**
     * Compacts a DataFrame using Spark's `coalesce`, optimizing storage with customizable file format,
     * block size, and sampling options.
     *
     * Usage Examples:
     * 1. Default compaction: {{{ dataframe.dataFrameCompactionWithCoalesce(sparkSession, FileFormat.OrcFileFormat, None) }}}
     * 2. Custom block size: {{{ dataframe.dataFrameCompactionWithCoalesce(sparkSession, FileFormat.OrcFileFormat, CustomBlockSize._128MB) }}}
     * 3. Custom block size and sample size: {{{ dataframe.dataFrameCompactionWithCoalesce(sparkSession, FileFormat.OrcFileFormat, CustomBlockSize._64MB, 500) }}
     * 4. Custom sample size: {{{ dataframe.dataFrameCompactionWithCoalesce(sparkSession, FileFormat.OrcFileFormat, None, 500) }}}
     *
     * Supported Block Sizes: [[org.scala.spark.file.enums.compaction.AdaptiveBlockSize]]
     * Supported File Formats: [[org.scala.spark.file.enums.compaction.SupportedFileFormat]]
     *
     * @param sparkSession          The active Spark session
     * @param destinationFileFormat Output file format
     * @param blockSize             Optional max file size for output (default: None)
     * @param recordsToSampled      Number of records to sample for size estimation (default: 1000)
     * @return Compacted DataFrame
     */
    def dataFrameCompactionWithCoalesce(sparkSession: SparkSession, destinationFileFormat: FileFormat, blockSize: Option[CustomBlockSize],
                                        recordsToSampled: Int = 1000): DataFrame =
      dataFrame.coalesce(fetchMaxFileCount(dataFrame, sparkSession, destinationFileFormat, blockSize, recordsToSampled))

    /**
     * Compacts a DataFrame using Spark's `repartition`, enabling efficient storage with configurable
     * file format, block size, and sampling options.
     *
     * Usage Examples:
     * 1. Default compaction: {{{ dataframe.dataFrameCompactionWithRepartition(sparkSession, FileFormat.OrcFileFormat, None) }}}
     * 2. Custom block size: {{{ dataframe.dataFrameCompactionWithRepartition(sparkSession, FileFormat.OrcFileFormat, Some(CustomBlockSize._64MB)) }}}
     * 3. Custom block size and sample size: {{{ dataframe.dataFrameCompactionWithRepartition(sparkSession, FileFormat.OrcFileFormat, Some(CustomBlockSize._64MB), 500) }}}
     * 4. Custom sample size: {{{ dataframe.dataFrameCompactionWithRepartition(sparkSession, FileFormat.OrcFileFormat, None, 500) }}}
     *
     * Supported Block Sizes: [[org.scala.spark.file.enums.compaction.AdaptiveBlockSize]]
     * Supported File Formats: [[org.scala.spark.file.enums.compaction.SupportedFileFormat]]
     *
     * @param sparkSession          Active Spark session
     * @param destinationFileFormat Output file format
     * @param blockSize             Optional max file size for output (default: None)
     * @param recordsToSampled      Number of records to sample for size estimation (default: 1000)
     * @return Compacted DataFrame
     */
    def dataFrameCompactionWithRepartition(sparkSession: SparkSession, destinationFileFormat: FileFormat, blockSize: Option[CustomBlockSize],
                                           recordsToSampled: Int = 1000): DataFrame =
      dataFrame.repartition(fetchMaxFileCount(dataFrame, sparkSession, destinationFileFormat, blockSize, recordsToSampled))
  }

  /**
   * Calculates the maximum number of files to be written to the destination path based on the DataFrame size.
   *
   * @param dataFrame             The DataFrame for which to calculate the number of output files
   * @param sparkSession          The active Spark session
   * @param destinationFileFormat The format of the output files
   * @param blockSize             The maximum size of a single output file
   * @param recordsToSampled      The subset of data used to estimate the DataFrame size
   * @return The maximum number of files to be written
   */
  def fetchMaxFileCount(dataFrame: DataFrame, sparkSession: SparkSession, destinationFileFormat: FileFormat,
                        blockSize: Option[CustomBlockSize], recordsToSampled: Int): Int = {
    val countOfDataFrame: Long = dataFrame.count()
    if (countOfDataFrame == 0) {
      1
    } else {
      val compressionRatio = computeCompressionRatio(destinationFileFormat)
      val optimisedSizeOfData = calculateOptimizedSize(dataFrame, sparkSession, compressionRatio, countOfDataFrame, recordsToSampled)

      val blockSizeOfFile = blockSize.getOrElse(determineBlockSize(optimisedSizeOfData)).id

      val noOfFilesToBeGenerated = (optimisedSizeOfData / blockSizeOfFile.toDouble).ceil.toInt

      noOfFilesToBeGenerated
    }
  }

  /**
   * Computes the optimized size of the input DataFrame based on the compression ratio, the total record count,
   * and the number of sampled records.
   *
   * The `recordsToSampled` parameter is used to estimate the DataFrame size:
   * - **Importance**: To determine the size from an optimized plan, we cache a subset of records (`recordsToSampled`)
   * instead of the entire DataFrame. This provides a near-approximate size without incurring the overhead of caching all records.
   *
   * The `sizeOfCachedDfInBytes` parameter represents the approximate size in bytes of the cached sampled records.
   *
   * The optimized size is calculated using the formula:
   *
   * ((sizeOfCachedDfInByte / recordsToBeSampled) * countOfDataframe) / compressionRatio
   *
   * @param dataFrame        The DataFrame to be written
   * @param sparkSession     The active Spark session
   * @param compressionRatio The compression ratio based on the file format
   * @param countOfDataFrame The total number of records in the input DataFrame
   * @param recordsToSampled The number of rows to sample for size estimation
   * @return The optimized size of the data in bytes
   */
  def calculateOptimizedSize(dataFrame: DataFrame, sparkSession: SparkSession, compressionRatio: Int, countOfDataFrame: Long, recordsToSampled: Int): Long = {
    val recordsToBeSampled = (if (recordsToSampled > 0) {
      Math.min(recordsToSampled.toLong, countOfDataFrame)
    } else {
      val defaultRecordsToBeSampledForCompaction = 1000
      Math.min(countOfDataFrame, defaultRecordsToBeSampledForCompaction.toLong)
    }).toInt

    val sizeOfCachedDfInBytes = sparkSession.sessionState.executePlan(dataFrame.limit(recordsToBeSampled).cache().queryExecution.logical).optimizedPlan.stats.sizeInBytes.longValue()

    ((sizeOfCachedDfInBytes / recordsToBeSampled) * countOfDataFrame) / compressionRatio
  }

  /**
   * Computes the optimal block size based on the size of the input DataFrame being written to the destination.
   *
   * Example calculations:
   * - Input: 10 MB → Output: 32 MB
   * (If input size is less than 64 MB, return 32 MB as block size)
   *
   * - Input: 100 MB → Output: 64 MB
   * (If input size is greater than 64 MB, return 64 MB as block size)
   *
   * - Input: 50 MB → Output: 32 MB
   * (If input size is less than 64 MB, return 32 MB as block size)
   *
   * @param optimisedSizeOfData The optimized size of the DataFrame
   * @return The optimal block size as a CustomBlockSize
   */
  def determineBlockSize(optimisedSizeOfData: Long): CustomBlockSize = {
    if (optimisedSizeOfData > 64000000L)
      AdaptiveBlockSize._64MB
    else
      AdaptiveBlockSize._32MB
  }

  /**
   * Calculates the compression ratio based on the destination file format (e.g., ORC, JSON, text, CSV, or Parquet).
   *
   * The compression ratio is determined through experimentation with multiple datasets.
   *
   * @param destinationFileFormat The format of the file written to the destination
   * @return The compression ratio as an integer
   */
  def computeCompressionRatio(destinationFileFormat: FileFormat): Int = {
    destinationFileFormat match {
      case Orc => 8
      case Json => 5
      case Text => 1
      case Csv => 1
      case Parquet => 8
    }
  }
}
