package uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor

class IgnoreAfterPipe private extends KeyPreProcessor with Serializable {
  override def keyPreProcessor: String => String = { key: String => key.split("[|]")(0) }
}
object IgnoreAfterPipe {
  def apply(): IgnoreAfterPipe = new IgnoreAfterPipe()
}