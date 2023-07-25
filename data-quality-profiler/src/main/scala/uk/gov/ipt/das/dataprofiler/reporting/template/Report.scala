package uk.gov.ipt.das.dataprofiler.reporting.template

import uk.gov.ipt.das.dataprofiler.feature.FeatureCollection
import uk.gov.ipt.das.dataprofiler.profiler.rule.RecordSets
import uk.gov.ipt.das.dataprofiler.reporting.template.model.{BasicReportTemplate, SourceInfo}

import java.time.LocalDate

class Report private (templateValues: BasicReportTemplate) {
  def withCustomLogo(dataURL: String): Report = new Report(templateValues.copy(logo_image_dataurl = dataURL))
}
object Report {
  def apply(title: String, recordSets: RecordSets, featureCollection: FeatureCollection): Option[Report] = {
    import featureCollection.featurePoints.sparkSession.implicits._
    MetricsReport.generateReportTemplate(featureCollection.getMetrics())
      .map { reportTemplate =>
        new Report(
          BasicReportTemplate(
            title = title,
            date = LocalDate.now().toString,
            sources = recordSets.recordSets.map { case (sourceName, records) =>
              SourceInfo(
                name = sourceName,
                unique_field_count = records.records.flatMap { fpr =>
                  fpr.flatValues.map { fv => fv.flatPath }.distinct
                }.distinct().count().toString,
                record_count = records.records.count().toString
              )
            }.toArray,
            rows = reportTemplate.toArray
          )
        )
      }
  }
}