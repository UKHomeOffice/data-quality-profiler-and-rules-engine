package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.feature.FeaturePoint
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler

/**
 * A field-based mask to run on a single value.
 *
 * If pathFilters is None, run on all keys.
 *
 * If pathFilters is present, this mask will run on all of the matches.
 *
 * if arrayQueryFilters is present it will add the masks with CSS style array matches
 *
 * Each of the MaskProfilers in profilers will be run over the value.
 *
 * @param pathFilters
 * @param profilers
 * @param queryFilters
 */
case class FieldBasedMask private (profilers: MaskProfiler*) extends ProfileRule {

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] =
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>

        val finalRecords  = filterByQueryPath match {
          case Some(queryPath) => records.filterByPaths(filterByPaths).records
            .union(records.withArrayQueryPaths(queryPath).records)
          case None => records.filterByPaths(filterByPaths).records
        }

        recordSet -> finalRecords.flatMap { record =>
          record.flatValues.flatMap { flatValue =>
            profilers.map { profiler =>
              feature.FeaturePoint(
                recordId = record.id,
                path = flatValue.flatPath,
                originalValue = flatValue.recordValue.valueAsString, // TODO implement non-string originalValue ?
                feature = profiler.getFeatureOutput(flatValue.recordValue),
                recordSet = recordSet,
                additionalIdentifiers = record.additionalIdentifiers
              )
            }
          }
        }
    }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = true
  override def allowsArrayQueryPaths:Boolean = true
}
object FieldBasedMask {
  def apply(profilers: MaskProfiler*): FieldBasedMask =
    new FieldBasedMask(profilers = profilers:_*)

  def apply(): FieldBasedMask =
    new FieldBasedMask(profilers = MaskProfiler.defaultProfilers:_*)
}