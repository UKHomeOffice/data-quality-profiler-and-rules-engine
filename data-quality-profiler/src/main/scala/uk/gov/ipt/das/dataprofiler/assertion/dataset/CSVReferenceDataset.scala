package uk.gov.ipt.das.dataprofiler.assertion.dataset

import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.SparkSession

import java.io.{InputStream, InputStreamReader}
import scala.collection.JavaConverters._

object CSVReferenceDataset {

  def fromFile(spark: SparkSession,
               srcFileInputStream: InputStream,
               valueColumnNumber: Int,
               datasetIDColumnNumber: Int,
               header: Boolean = false): Map[String, ReferenceDataset] = {

    import spark.implicits._

    val csvReader = if (header) CSVFormat.DEFAULT.withFirstRecordAsHeader() else CSVFormat.DEFAULT

    csvReader.parse(new InputStreamReader(srcFileInputStream)).asScala.toSeq
      .map { record =>
        (record.get(datasetIDColumnNumber), record.get(valueColumnNumber))
      }
      .groupBy{ case (datasetName, _) => datasetName }
      .map{ case (datasetName, valuePairs) =>
        datasetName -> valuePairs.map{ _._2 }
      }
      .map { case (datasetName, values) =>
        datasetName -> ReferenceDataset(spark.createDataset(values))
      }
  }

  def fromFile(spark: SparkSession, srcFile: String, columnNumber: Int): ReferenceDataset = {
    import spark.implicits._

    ReferenceDataset(
      spark.read
        .option("header", "true")
        .csv(srcFile)
        .map { _.getString(columnNumber) }
    )
  }

  def fromStreamedFile(spark: SparkSession,
                       srcFileInputStream: InputStream,
                       columnNumber: Int,
                       header: Boolean = false): ReferenceDataset = {
    import spark.implicits._

    val csvReader = if (header) CSVFormat.DEFAULT.withFirstRecordAsHeader() else CSVFormat.DEFAULT
    ReferenceDataset( { csvReader.parse(new InputStreamReader(srcFileInputStream))
      .asScala.toSeq.map { record =>
        record.get(columnNumber)}.toDS
    })
  }
}