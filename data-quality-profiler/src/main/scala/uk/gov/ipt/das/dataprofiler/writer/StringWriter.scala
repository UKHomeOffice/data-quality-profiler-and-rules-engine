package uk.gov.ipt.das.dataprofiler.writer

import org.apache.commons.io.IOUtils
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame

import java.io.Reader

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements","org.wartremover.warts.Var"))
class StringWriter private extends NamedFileWriter with DataFrameWriter {

  private var lastReport: Option[String] = None

  private var lastDataFrame: Option[String] = None
  def getLastDataFrame: Option[String] = lastDataFrame

  override def writeNamedFile(filename: String, contents: Reader): Unit =
    lastReport = Option(IOUtils.toString(contents))

  override def writeDataFrame(dataFrame: VersionedDataFrame, partitionOn: Option[Seq[String]]): Unit = {
    dataFrame.dataFrame.fold {
      lastDataFrame = Option("")
    } {
      df => lastDataFrame = Option(df.collect().mkString("Array(", ", ", ")"))
    }
  }
}
object StringWriter {
  def apply(): StringWriter = new StringWriter
}