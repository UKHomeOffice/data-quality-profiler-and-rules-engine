package uk.gov.ipt.das.dataprofiler.assertion.dataset

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import ReferenceDataset.FIELD_REFVALUE

import java.util.{Base64, UUID}
import scala.collection.immutable.HashSet

case class ReferenceDataset private (df: DataFrame, view: String) {
  def refValue: String = s"$view.$FIELD_REFVALUE"

  private lazy val set: HashSet[Array[String]] = HashSet(df.collect().map(row => row.getString(0)))

  def asSet: HashSet[String] = set.flatten

}

object ReferenceDataset {

  val FIELD_REFVALUE = "refValue"

  /**
   * Generates a Base64 encoding of a UUID.
   * @return random view name as a 22 char string
   */
  def randomViewName: String = {
    def asByteArray(uuid: UUID) = {
      val msb = uuid.getMostSignificantBits
      val lsb = uuid.getLeastSignificantBits
      val buffer = new Array[Byte](16)
      for (i <- 0 until 8) {
        buffer(i) = (msb >>> 8 * (7 - i)).toByte
      }
      for (i <- 8 until 16) {
        buffer(i) = (lsb >>> 8 * (7 - i)).toByte
      }
      buffer
    }

    val s = Base64.getEncoder.encodeToString(asByteArray(UUID.randomUUID()))
    s.split("=")(0).replaceAll("[/]", "").replaceAll("[+]", "")
  }

  def apply(dataset: Dataset[String]): ReferenceDataset = {
    val view = randomViewName
    val df = dataset.toDF(FIELD_REFVALUE)
    df.createOrReplaceTempView(view)
    new ReferenceDataset(df = df, view = view)
  }
}