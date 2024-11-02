package org.scala.spark.file.enums.compaction

/**
 * Enumeration of file formats supported for compaction.
 */
object SupportedFileFormat extends Enumeration {
  type FileFormat = Value
  val Text, Orc, Csv, Parquet, Json = Value
}
