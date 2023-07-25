package uk.gov.ipt.das.dataprofiler.identifier.processor

import uk.gov.ipt.das.dataprofiler.identifier.IdentifierProcessor

class DirectlyMapped private () extends IdentifierProcessor {
  override def process(value: String): String = value
}
object DirectlyMapped {
  lazy val INSTANCE = new DirectlyMapped
  def apply(): DirectlyMapped = INSTANCE
}