package uk.gov.ipt.das.dataprofiler.reporting.template.model

import BasicReportTemplate.{imageToDataURL, performRedact}
import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.ImageToDataURL

case class BasicReportTemplate private (title: String,
                                        date: String,
                                        sources: Array[SourceInfo],
                                        rows: Array[BasicReportRowTemplate],
                                        logo_image_dataurl: String,
                                        bottom_left_logo: String,
                                        sample_css_style: String) {

  def withLogoFromResource(resourcePath: String, imageMimeType: String): BasicReportTemplate =
    this.copy(logo_image_dataurl =
      imageToDataURL(getClass.getClassLoader.getResourceAsStream(resourcePath), imageMimeType))

  def withJPEGLogoFromResource(resourcePath: String): BasicReportTemplate =
    withLogoFromResource(resourcePath, "jpg")

  def withPNGLogoFromResource(resourcePath: String): BasicReportTemplate =
    withLogoFromResource(resourcePath, "png")

  def redacted: BasicReportTemplate = this.copy(rows = performRedact(rows))

}

object BasicReportTemplate extends ImageToDataURL {

  private lazy val defaultLogo: String = imageToDataURL(getClass.getClassLoader.getResourceAsStream("html-report/Home-Office_4CP_AW-4-620x267"), "png")
  private lazy val defaultBottomLeftLogo: String = imageToDataURL(getClass.getClassLoader.getResourceAsStream("html-report/6point6.png"), "png")

  def performRedact(rows: Array[BasicReportRowTemplate]): Array[BasicReportRowTemplate] =
    rows.map { row =>
      row.copy(
        top10_hg = row.top10_hg.map(_.copy(sample = "REDACTED")),
        top10_lg = row.top10_lg.map(_.copy(sample = "REDACTED")),
        bottom10_hg = row.bottom10_hg.map(_.copy(sample = "REDACTED")),
        bottom10_lg = row.bottom10_lg.map(_.copy(sample = "REDACTED"))
      )
    }

  private def redactRows(rows: Array[BasicReportRowTemplate])(implicit doRedact: Boolean): Array[BasicReportRowTemplate] =
    if (doRedact) performRedact(rows) else rows

  def apply(title: String,
            date: String,
            sources: Array[SourceInfo],
            rows: Array[BasicReportRowTemplate],
            logo_image_dataurl: String = defaultLogo,
            bottom_left_logo: String = defaultBottomLeftLogo)
           (implicit redactSamples: Boolean = false): BasicReportTemplate =
    new BasicReportTemplate(
      title = title,
      date = date,
      sources = sources,
      rows = redactRows(rows.sortBy(row => row.json_path)),
      logo_image_dataurl = logo_image_dataurl,
      bottom_left_logo = bottom_left_logo: String,
      sample_css_style = if (redactSamples) "display: none;" else ""
    )

}