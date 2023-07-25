package uk.gov.ipt.das.dataprofiler.dataframe

import org.apache.spark.sql.DataFrame
import uk.gov.ipt.das.dataprofiler.writer.DataFrameWriter

trait VersionedDataFrame {

  def dataFrame: Option[DataFrame]

  def schemaVersion: String

  def name: String

  def versionedFolder: String = s"$name--$schemaVersion"

  def write(writer: DataFrameWriter, partitionOn: Option[Seq[String]]): Unit =
    writer.writeDataFrame(this, partitionOn = partitionOn)

}
