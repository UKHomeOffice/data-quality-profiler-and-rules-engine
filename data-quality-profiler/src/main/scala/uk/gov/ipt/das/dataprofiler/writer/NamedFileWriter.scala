package uk.gov.ipt.das.dataprofiler.writer

import java.io.Reader

trait NamedFileWriter {
  def writeNamedFile(filename: String, source: Reader): Unit
}