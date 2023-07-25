package uk.gov.ipt.das.dataprofiler.assertion.dataset

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.SparkSession
import uk.gov.ipt.das.mixin.ExcelMixins

import java.io.InputStream
import scala.collection.JavaConverters._

object ExcelReferenceDataset extends ExcelMixins {

  /**
   * Read multiple datasets from an Excel spreadsheet.
   *
   * @param spark SparkSession to use to read the file
   * @param srcFile Filename of the source spreadsheet
   * @param worksheetName Worksheet to read from
   * @param datasetColumnNumber Column letter to read the dataset name from (i.e. the key of the Map output)
   * @param valueColumnNumber Column letter to read the value from
   * @return
   */
  def fromFile(spark: SparkSession,
               srcFile: InputStream,
               worksheetName: String,
               datasetColumnNumber: Int,
               valueColumnNumber: Int,
               ignoreTopRows: Int = 1): Map[String, ReferenceDataset] = {

    import spark.implicits._

    val workbook = new XSSFWorkbook(srcFile)
    val sheet = workbook.getSheet(worksheetName)

    val valuesByDataset = sheet.rowIterator().asScala
      .filter { row => row.getRowNum >= ignoreTopRows }
      .map { row =>
        getCellValueAsString(row.getCell(datasetColumnNumber)) -> getCellValueAsString(row.getCell(valueColumnNumber))
      }.toList
      .groupBy{ case (datasetName, values) => datasetName }
      .map { case (datasetName, valuePairs) => datasetName -> valuePairs.map{ _._2 } }

    valuesByDataset.map { case (datasetName, values) =>
      datasetName -> ReferenceDataset(spark.createDataset(values))
    }
  }

}
