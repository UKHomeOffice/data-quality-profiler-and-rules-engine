package uk.gov.ipt.das.dataprofiler.reporting

case class DomainOfInterest(name: String,
                            filters: Seq[MetricsFilter])
