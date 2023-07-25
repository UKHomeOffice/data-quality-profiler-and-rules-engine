package uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor

import uk.gov.ipt.das.reflection.ClassReflection

trait KeyPreProcessor {
  def keyPreProcessor: String => String
}
object KeyPreProcessor extends ClassReflection {
  def byName(name: String, pkg: String = "uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor"): KeyPreProcessor =
    instanceByClassName[KeyPreProcessor](name = name, pkg = pkg)
}