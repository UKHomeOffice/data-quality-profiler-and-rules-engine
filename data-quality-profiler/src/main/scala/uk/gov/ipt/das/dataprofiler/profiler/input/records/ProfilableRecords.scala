package uk.gov.ipt.das.dataprofiler.profiler.input.records

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord

class ProfilableRecords private (recordsIn: Dataset[ProfilableRecord]) {
  def records: Dataset[ProfilableRecord] = recordsIn
}
object ProfilableRecords {
  def apply(records: Dataset[ProfilableRecord]): ProfilableRecords = new ProfilableRecords(records)
}
