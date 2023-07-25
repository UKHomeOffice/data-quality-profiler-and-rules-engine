package uk.gov.ipt.das.dataprofiler.reporting.template.model

import org.apache.spark.sql.SparkSession
import uk.gov.ipt.das.dataprofiler.profiler.rule.RecordSets

case class SourceInfo(name: String,
                      unique_field_count: String,
                      record_count: String)

object SourceInfo {
  def fromRecordSets(recordSets: RecordSets)(implicit spark: SparkSession): Array[SourceInfo] = {
    import spark.implicits._
    recordSets.recordSets.map { case (sourceName, records) =>
      SourceInfo(
        name = sourceName,
        unique_field_count = records.records.flatMap { fpr =>
          fpr.flatValues.map { fv => fv.flatPath }.distinct
        }.distinct().count().toString,
        record_count = records.records.count().toString
      )
    }.toArray
  }
}