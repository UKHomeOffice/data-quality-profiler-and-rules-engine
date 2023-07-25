package uk.gov.ipt.das.dataprofiler.identifier

import uk.gov.ipt.das.reflection.ClassReflection

trait IdentifierProcessor {
  def process(value: String): String
}
object IdentifierProcessor extends ClassReflection {
  def byName(name: String, pkg: String = "uk.gov.ipt.das.dataprofiler.identifier.processor"): IdentifierProcessor =
    instanceByClassName[IdentifierProcessor](name = name, pkg = pkg)
}