package org.scala.spark.file.enums.compaction

/**
 * Returns the predefined block size used for compaction.
 */
object AdaptiveBlockSize extends Enumeration {
  type CustomBlockSize = Value
  val _32MB: AdaptiveBlockSize.Value = Value(32000000)
  val _64MB: AdaptiveBlockSize.Value = Value(64000000)
  val _128MB: AdaptiveBlockSize.Value = Value(128000000)
}
