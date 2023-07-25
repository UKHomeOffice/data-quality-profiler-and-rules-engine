package uk.gov.ipt.das.dataprofiler.feature.combiner

import uk.gov.ipt.das.dataprofiler.feature.{ComparableFeaturePoint, FeatureReportRow}

case class Combined(
                     min: String,
                     max: String,
                     first: String,
                     last: String,
                     count: Long
                   ) {

  def null2Empty(in: String): String = {
    if (in != null) in else ""
  }

  def min(lStrIn: String, rStrIn: String): String = {
    val lStr = null2Empty(lStrIn)
    val rStr = null2Empty(rStrIn)
    if (lStr < rStr) lStr else rStr
  }

  def max(lStrIn: String, rStrIn: String): String = {
    val lStr = null2Empty(lStrIn)
    val rStr = null2Empty(rStrIn)
    if (lStr > rStr) lStr else rStr
  }

  def reduce(second: Combined): Combined = {
    Combined(
      min = min(min, second.min),
      max = max(max, second.max),
      first = first,
      last = second.last,
      count = count + second.count
    )
  }

  def toFeatureReportRow(featurePoint: ComparableFeaturePoint): FeatureReportRow = {
    FeatureReportRow(
      featurePoint = featurePoint,
      sampleMin = min,
      sampleMax = max,
      sampleFirst = first,
      sampleLast = last,
      count = count
    )
  }
}
object Combined {
  def make(s: String): Combined = Combined(s,s,s,s,1L)
}