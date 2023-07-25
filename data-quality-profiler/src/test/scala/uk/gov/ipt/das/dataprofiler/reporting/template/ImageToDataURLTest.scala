package uk.gov.ipt.das.dataprofiler.reporting.template

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.reporting.template.model.BasicReportTemplate.imageToDataURL

class ImageToDataURLTest extends AnyFunSpec {

  it("Loads an image to a data URL") {
    val stream = getClass.getClassLoader.getResourceAsStream("html-report/Home-Office_4CP_AW-4-620x267.jpg")

    assert(stream != null)

    val dataURL = imageToDataURL(stream, "png")

    println(dataURL)

    val expectedPrefix = "data:image/png;base64,"

    assert(dataURL.startsWith(expectedPrefix))
    assert(dataURL.length > expectedPrefix.length)
  }

}
