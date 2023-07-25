package uk.gov.ipt.das.dataprofiler.profiler.rule

import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords

import scala.collection.immutable.Map

case class RecordSets private (recordSets: Map[String, FlattenedRecords]) {

  def filter(recordSetKeysOpts: Option[Seq[String]]): RecordSets =
    RecordSets(recordSetKeysOpts.fold(recordSets.keys){ recordSetKeys => recordSetKeys }.map { key =>
      key -> recordSets(key)
    }.toMap)

}
object RecordSets {
  def apply(elems: (String, FlattenedRecords)*): RecordSets =
    new RecordSets(recordSets = Map[String, FlattenedRecords](elems: _*))
}
