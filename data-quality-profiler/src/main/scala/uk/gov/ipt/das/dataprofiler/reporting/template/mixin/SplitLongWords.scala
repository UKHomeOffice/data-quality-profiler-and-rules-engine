package uk.gov.ipt.das.dataprofiler.reporting.template.mixin

trait SplitLongWords {

  private val maxWordSize: Int = 25
  val ZERO_WIDTH_SPACE: String = "\u200B" // mustache converts this to &#8203; for us. if we use &#8203; it will be escaped.

  /**
   * Split words longer than 25 chars by inserting a hidden space (&#8203;) so that the HTML
   * report can wrap this word.
   */
  def slw(s: String): String =
    s.split(' ').map { word =>
      word.grouped(maxWordSize).mkString(ZERO_WIDTH_SPACE)
    }.mkString(" ")

  /**
   * Split words longer than 25 chars, but try to split them on a "." - use this for
   * JSON paths that have loads of "." in them, using hidden space (&#8203;)
   */
  def slwPath(s: String): String =
    s.split(' ').map { word =>
      if (word.length > maxWordSize) {
        word.replaceAll("[.]", s".$ZERO_WIDTH_SPACE")
      } else {
        word
      }
    }.mkString(" ")
}
