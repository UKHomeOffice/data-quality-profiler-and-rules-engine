package uk.gov.ipt.das.dataprofiler.reporting.charts

import org.slf4j.{Logger, LoggerFactory}
import HistogramChart.{logger, pixelDataToPNG, quantize, valuesTo2DArray}

import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import scala.annotation.tailrec

/**
 * A histogram chart created from a list of values.
 *
 * @param valuesIn Each value, from largest to smallest, pre-sorted
 * @param repeatMax Repeat pixels if the number of width pixels is less than this
 * @param quantizeMin Quantize the result if the number of width pixels is greater than this
 */
class HistogramChart(valuesIn: Seq[Long], repeatMax: Int = 500, quantizeMin: Int = 4096) {

  logger.debug(s"HistogramChart, valuesIn: ${valuesIn.mkString("(","L,","L)")}")

  def asPNG: Array[Byte] = pixelDataToPNG(valuesIn, repeatMax = repeatMax, quantizeMin = quantizeMin)

}
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements","org.wartremover.warts.IterableOps"))
object HistogramChart {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def quantize(valuesIn: Seq[Long], quantizeBase: Int): Seq[Long] = {
    // figure out how many bars to (equally) combine to get the number below $quantizeBase
    val barsToCombine = Math.ceil(valuesIn.length.toDouble / quantizeBase.toDouble).toInt
    valuesIn.grouped(barsToCombine).map{ vals => vals.sum.toDouble / vals.length.toDouble }.map(Math.ceil(_).toLong).toSeq
  }

  /**
   * Take binary pixel data and repeat each vertical bar to double the width of the image
   *
   * @param pixelData Pixel on/off values in a 2d array
   * @return 2d array with twice as many columns
   */
  @tailrec
  def repeatVerticalBars(pixelData: Array[Array[Boolean]], minWidth: Int): Array[Array[Boolean]] = {
    logger.debug(s"repeatVerticalBars, pixelData.length < minWidth, pixelData.length: ${pixelData.length}, minWidth: $minWidth")

    /**
     * e.g.  [A,B,C,D] => [A,A,B,B,C,C,D,D]
     */
    if (pixelData.length < minWidth) {
      val newPixelData = Array.ofDim[Boolean](pixelData.length * 2, pixelData(0).length)
      pixelData.zipWithIndex.foreach { case (verticalPixels, w) =>
        newPixelData(w * 2) = verticalPixels
        newPixelData((w * 2) + 1) = verticalPixels
      }
      repeatVerticalBars(newPixelData, minWidth = minWidth) // recurse until minWidth is reached
    } else {
      pixelData
    }
  }


  /**
   * Convert raw bars into "N black + 1 white padding" arrays
   *
   * Remove last 1 pixel pad so it ends on the final bar.
   */
  def whitespaceVerticalBars(pixelData: Array[Array[Boolean]], barWidth: Int = 5): Array[Array[Boolean]] = {
    // padWidth is the new array-space taken up by a single bar
    // (i.e. the new width of a bar plus 1 pixel for the white space)
    val padWidth = barWidth + 1

    val newPixelData = Array.ofDim[Boolean](pixelData.length * padWidth - 1, pixelData(0).length)
    pixelData.zipWithIndex.foreach { case (verticalPixels, w) =>
      (0 until barWidth).foreach { i =>
        newPixelData((w * padWidth) + i) = verticalPixels
      }
      if (w < pixelData.length - 1) { // don't add padding to the last bar
        newPixelData((w * padWidth) + barWidth) = Array.ofDim[Boolean](verticalPixels.length).map(_ => false)
      }
    }
    newPixelData
  }

  /**
   * Data here must be final, i.e. padded with bar gaps and repated where necessary
   */
  def paddedDataToPNG(pixelData: Array[Array[Boolean]]): Array[Byte] = {
    val image = new BufferedImage(pixelData.length, pixelData.head.length, BufferedImage.TYPE_BYTE_BINARY)
    pixelData.zipWithIndex.map { case (verticalPixels, w) =>
      verticalPixels.zipWithIndex.map { case (pixelValue, h) =>
        image.setRGB(w, h, if (pixelValue) 0x000000 else 0xFFFFFF)
      }
    }
    val baos = new ByteArrayOutputStream()
    ImageIO.write(image, "png", baos)
    baos.toByteArray
  }

  /**
   * Pad out bars first, and then apply a repeat to reach minWidth, then convery to PNG
   * @param values 2d array of boolean pixel data, outer array is left-right, inner array is top-bottom histogram bar
   * @param repeatMax minimum width to repeat left-right pixels to
   * @param quantizeMin minimum width to quantize the data, and not perform the whitespacing
   * @return PNG image as a byte array
   */
  def pixelDataToPNG(values: Seq[Long], repeatMax: Int, quantizeMin: Int): Array[Byte] = {

    val resized = if (values.lengthCompare(quantizeMin) > 0) {
      // no "padVerticalBars" here, as we want no extra pixels for this amount of data
      valuesTo2DArray(quantize(values, quantizeMin))
    } else if (values.lengthCompare(repeatMax) < 0) {
      // we do "repeatVerticalBars" here to make the pixel width a size where the HTML rendered doesn't blur
      repeatVerticalBars(whitespaceVerticalBars(valuesTo2DArray(values)), repeatMax)
    } else {
      // just add the white spacing between bars
      whitespaceVerticalBars(valuesTo2DArray(values))
    }

    // convert to PNG
    paddedDataToPNG(resized)
  }

  /**
   *
   * @param values sorted histogram values, largest to smallest
   * @param height how tall to make the image
   * @return 2d array of boolean pixel data, outer array is left-right, inner array is top-bottom histogram bar
   */
  def valuesTo2DArray(values: Seq[Long], height: Int = 100): Array[Array[Boolean]] = {
    val max = values.max

    // ex:
    // max = 1000, height = 1000, factor = 1.0
    // max = 100, height = 1000, factor = 10.0
    // max = 10000, height = 1000, factor = 0.1

    // multiply by this factor to get the height of the bar
    val factor = height.toDouble / max.toDouble

    val normalisedBarHeights = values.sorted.reverse.map { value =>
      Math.ceil(value.toDouble * factor)
    }

    normalisedBarHeights.map { bar =>
      (0 until height).map { vertPixel =>
        bar >= height - vertPixel
      }.toArray
    }.toArray
  }

}