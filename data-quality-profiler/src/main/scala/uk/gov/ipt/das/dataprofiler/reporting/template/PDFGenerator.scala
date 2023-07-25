package uk.gov.ipt.das.dataprofiler.reporting.template

import com.itextpdf.html2pdf.{ConverterProperties, HtmlConverter}
import com.itextpdf.kernel.geom.PageSize
import com.itextpdf.kernel.pdf.{PdfDocument, PdfWriter}
import com.itextpdf.styledxmlparser.css.media.{MediaDeviceDescription, MediaType}

import java.io.{ByteArrayOutputStream, File, FileOutputStream, OutputStream}

object PDFGenerator {

  /**
   * Generate a landscape A4 PDF from HTML, suitable for creating HTML reports.
   *
   * @param htmlString HTML source, single file with embedded CSS.
   * @param outputStream Output stream where the PDF will be written
   */
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def generateToStream(htmlString: String, outputStream: OutputStream): Unit = {
    val pdfDocument = new PdfDocument(new PdfWriter(outputStream))

    // Landscape document
    pdfDocument.setDefaultPageSize(PageSize.A4.rotate)

    val properties = new ConverterProperties()
    // required for "page X of Y" in CSS print definitions
    properties.setMediaDeviceDescription(new MediaDeviceDescription(MediaType.PRINT))

    HtmlConverter.convertToPdf(htmlString, pdfDocument, properties)
  }

  def generateToFile(htmlString: String, outputFile: File): Unit =
    generateToStream(htmlString, new FileOutputStream(outputFile))

  def generateToByteArray(htmlString: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    generateToStream(htmlString, outputStream = baos)
    baos.toByteArray
  }


}
