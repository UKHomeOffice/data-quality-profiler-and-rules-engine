package uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor

class IgnoreAfterPeriod private extends KeyPreProcessor with Serializable {
  override def keyPreProcessor: String => String = { key: String => key.split("[.]")(0) }
}
object IgnoreAfterPeriod {
  def apply(): IgnoreAfterPeriod = new IgnoreAfterPeriod()
}