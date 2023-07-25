package uk.gov.ipt.das.dataprofiler.reporting.template.charts

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.reporting.charts.HistogramChart
import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.ImageToDataURL

import java.io.FileOutputStream
import java.nio.file.Files

class HistogramChartTests extends AnyFunSpec with ImageToDataURL {
  it("generates values array from values correctly without scaling") {
    val values = List(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)

    val expected = Array(
      Array(true, true, true, true, true, true, true, true, true, true), // "10"
      Array(false, true, true, true, true, true, true, true, true, true), // "9"
      Array(false, false, true, true, true, true, true, true, true, true), // "8"
      Array(false, false, false, true, true, true, true, true, true, true), // "7"
      Array(false, false, false, false, true, true, true, true, true, true), // "6"
      Array(false, false, false, false, false, true, true, true, true, true), // "5"
      Array(false, false, false, false, false, false, true, true, true, true), // "4"
      Array(false, false, false, false, false, false, false, true, true, true), // "3"
      Array(false, false, false, false, false, false, false, false, true, true), // "2"
      Array(false, false, false, false, false, false, false, false, false, true), // "1"
    )

    val unchanged = HistogramChart.valuesTo2DArray(values, height = 10)

    assert(expected === unchanged)
  }

  it("generates a PNG image from values") {
    val values = List(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)

    val chart = new HistogramChart(values)
    val pngBytes = chart.asPNG

    val tmpFile = Files.createTempFile(s"png-histogram", ".png").toFile
    val fos = new FileOutputStream(tmpFile)
    fos.write(pngBytes)
    fos.close()

    println(s"Output PNG image to ${tmpFile.getAbsolutePath}")

    val dataURL = imageToDataURL(pngBytes, "png")
    val expectedDataURL = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAA7AAAABkAQAAAACXgeHLAAAAaklEQVR42u3OMRLAIAwDQf//06RLxZAJGEOxqtTtRbxrhQssFoudYOsSsFgsdpXdm4DFYrGZbH4CFovF7mJzErBYLLaCnU/AYrHYavZfAhaLxZ5kvxOwWCz2FrafgMVisTey0RsWi8WO/gOiEoTNjy2l8wAAAABJRU5ErkJggg=="
    println(dataURL)
    assert(expectedDataURL === dataURL)
  }

  it("generates a PNG image from a realistic set of values") {
    val values = List(13L,17L,83L,25L,19L,21L,33L,36L,15L,23L,35L,37L)

    val chart = new HistogramChart(values)
    val pngBytes = chart.asPNG

    val tmpFile = Files.createTempFile(s"png-histogram", ".png").toFile
    val fos = new FileOutputStream(tmpFile)
    fos.write(pngBytes)
    fos.close()

    println(s"Output PNG image to ${tmpFile.getAbsolutePath}")

    val dataURL = imageToDataURL(pngBytes, "png")
    val expectedDataURL = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAjgAAABkAQAAAACj3Xf+AAAAXklEQVR42u3MQQ5AQBAAwfn/p4kTwsqwkTVSfehjRSxN3QWHw+FwOBwOh8PhcDicltOt7ZwO7cR5pJVxbmoczotOWivpJLQBzqVW3mlog52D9glno/3QiTUOh8PJfwYCPSXmZWrM3wAAAABJRU5ErkJggg=="
    println(dataURL)
    assert(expectedDataURL === dataURL)
  }

  it("handles a high number of bars") {
    val barcount = 100000
    val values = (0 until barcount).map { rowNumber => rowNumber.toLong }.toList

    val chart = new HistogramChart(values)
    val pngBytes = chart.asPNG

    val tmpFile = Files.createTempFile(s"png-histogram", ".png").toFile
    val fos = new FileOutputStream(tmpFile)
    fos.write(pngBytes)
    fos.close()

    println(s"Output PNG image to ${tmpFile.getAbsolutePath}")

    val dataURL = imageToDataURL(pngBytes, "png")
    val expectedDataURL = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAD6AAAABkAQAAAAC6NfLkAAABO0lEQVR42u3RQREAIADDsPk3DccLEU0lNNvrqNR+ZgTRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfRyVfR4/KLB508dPLQyUMnD508dPLQyUMnD508dPLQyUMnD508dPLQyUMnD70qTygoDycozyUojyQoTyMoDyIozyAob39Q3vmgvOlBeb+D8lYH5V0OyhsclPc2KG9rUN7RoLyZQXkfg/IWBuXdC8obF5T3LChvV1DeqaC8SUF5f4Ly1gTlXQnKGxKU9yIob0OwC8wQUvOPPbN/AAAAAElFTkSuQmCC"
    println(dataURL)
    assert(expectedDataURL === dataURL)
  }

}
