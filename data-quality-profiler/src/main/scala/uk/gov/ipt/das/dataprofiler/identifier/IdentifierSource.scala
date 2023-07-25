package uk.gov.ipt.das.dataprofiler.identifier

case class IdentifierSource (sourcePath: String, processorName: String) {
  lazy val processor: IdentifierProcessor = IdentifierProcessor.byName(processorName)
}
object IdentifierSource {
  def direct(sourcePath: String): IdentifierSource = IdentifierSource(sourcePath, "DirectlyMapped")
}