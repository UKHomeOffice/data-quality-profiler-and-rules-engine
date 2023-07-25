package uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor

class PassthroughKeyProcessor private extends KeyPreProcessor with Serializable {
  override def keyPreProcessor: String => String = { key => key }
}
object PassthroughKeyProcessor {
  def apply(): PassthroughKeyProcessor = new PassthroughKeyProcessor()
}