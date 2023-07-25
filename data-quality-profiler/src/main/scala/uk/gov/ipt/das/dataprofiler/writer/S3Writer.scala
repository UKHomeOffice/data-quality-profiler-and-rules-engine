package uk.gov.ipt.das.dataprofiler.writer

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.commons.io.input.ReaderInputStream
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame

import java.io.Reader
import java.nio.charset.StandardCharsets

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Var"))
class S3Writer private (bucket: String,
                        reportFolder: String,
                        client: AmazonS3,
                        prefix: String,
                        suffix: String
                       ) extends NamedFileWriter with DataFrameWriter {

  override def writeNamedFile(filename: String, contents: Reader): Unit = {
    var fullReportPath = s"$prefix$filename$suffix"
    if(!StringUtils.isEmpty(reportFolder)) {
      fullReportPath = s"$reportFolder/$fullReportPath"
    }

    val metadata = new ObjectMetadata()
    client.putObject(bucket, fullReportPath, new ReaderInputStream(contents, StandardCharsets.UTF_8), metadata)
  }

  private def writeEmptyFile(outputPrefix: String): Unit = {
    client.putObject(bucket, outputPrefix, "")
  }

  override def writeDataFrame(dataFrame: VersionedDataFrame, partitionOn: Option[Seq[String]]): Unit = {
    dataFrame.dataFrame.fold {
      writeEmptyFile(s"$reportFolder/${dataFrame.versionedFolder}.NO_RECORDS")
    } { df =>
      val writer = partitionOn.fold {
        df
      } { partitions =>
        df.repartition(partitions.map {
          col
        }: _*)
      }
        .write.mode(SaveMode.Overwrite)

      partitionOn.fold {
        writer
      } { partitions =>
        writer.partitionBy(partitions: _*)
      }
        .parquet(s"s3://$bucket/$reportFolder/${dataFrame.versionedFolder}")
    }
  }
}
object S3Writer {
  def apply(bucket: String,
            reportFolder: String,
            prefix: String = "",
            suffix: String = "")(implicit client: AmazonS3)
    = new S3Writer(bucket, reportFolder, client, prefix, suffix)
}