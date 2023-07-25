package uk.gov.ipt.das.dataprofiler.writer

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame

import java.io.{File, FileWriter, Reader, StringReader}
import scala.collection.mutable.ArrayBuffer
@SuppressWarnings(Array("org.wartremover.warts.All"))
class LocalFileWriter private (folder: String, prefix: String, suffix: String)
  extends NamedFileWriter with DataFrameWriter {

  private val logger = LoggerFactory.getLogger(getClass)

  private var cleanLocation = folder
  if (!cleanLocation.endsWith("/")) cleanLocation += "/"

  private val namedFilenames: ArrayBuffer[String] = ArrayBuffer()
  def getNamedFilenames: Seq[String] = namedFilenames

  private val dataFrameLocations: ArrayBuffer[String] = ArrayBuffer()
  def getDataFrameLocations: Seq[String] = dataFrameLocations


  private def createOutputFolder(): Unit = {
    val outputFolder = new File(cleanLocation)
    if (!outputFolder.exists()) {
      logger.debug(s"Creating output folder: ${outputFolder.getAbsolutePath}")
      outputFolder.mkdirs()
    } else if (!outputFolder.isDirectory) {
      throw new Exception(s"Output folder is not a directory: ${outputFolder.getAbsolutePath}")
    }
  }

  override def writeNamedFile(filename: String, contents: Reader): Unit = {
    createOutputFolder()
    val fullPath = s"$cleanLocation$prefix$filename$suffix"
    logger.debug(s"Writing named file to $fullPath")

    val fileOut = new File(fullPath)
    if (fileOut.exists()) throw new Exception(s"Output named file $fullPath already exists, aborting.")

    val fw = new FileWriter(fileOut)
    IOUtils.copy(contents, fw)
    fw.close()

    namedFilenames += fullPath
  }

  override def writeDataFrame(dataFrame: VersionedDataFrame, partitionOn: Option[Seq[String]]): Unit = {
    // add schema version to output folder name
    val fullPath = s"$cleanLocation$prefix${dataFrame.versionedFolder}$suffix/"

    dataFrame.dataFrame.fold {
      val noRecordsPath = s"$fullPath.NO_RECORDS"
      writeNamedFile(noRecordsPath, new StringReader(""))
      dataFrameLocations += noRecordsPath
    }
    { df =>
      val writer = df
        .write
        .mode(SaveMode.Overwrite)

      partitionOn.fold {
        writer
      } { partitions =>
        writer.partitionBy(partitions: _*)
      }
        .parquet(fullPath)

      dataFrameLocations += fullPath
    }

  }

}
object LocalFileWriter {
  def apply(folder: String, prefix: String = "", suffix: String = ""): LocalFileWriter
    = new LocalFileWriter(folder, prefix, suffix)
}