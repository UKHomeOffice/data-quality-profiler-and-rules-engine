package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.value.RecordValue

case class FlatValue(flatPath: String,
                     fullyQualifiedPath: String,
                     recordValue: RecordValue)