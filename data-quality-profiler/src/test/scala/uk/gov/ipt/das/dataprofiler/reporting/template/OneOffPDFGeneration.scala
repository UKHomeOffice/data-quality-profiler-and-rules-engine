package uk.gov.ipt.das.dataprofiler.reporting.template

import org.apache.commons.io.IOUtils

import java.io.{File, FileReader}

object OneOffPDFGeneration extends App {
  if (args.length != 1) {
    println("Usage: OneOffPDFGeneration <html-report-html-filename>")
    System.exit(1)
  }

  val tmpPdfFile = File.createTempFile("basic-report", ".pdf")
  PDFGenerator.generateToFile(IOUtils.toString(new FileReader(args.head)), tmpPdfFile)
  println(s"PDF file output to ${tmpPdfFile.getAbsolutePath}")
}
