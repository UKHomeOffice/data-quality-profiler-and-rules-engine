package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.assertion.dataset.{CSVReferenceDataset, ExcelReferenceDataset}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CSVReferenceDatasetTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("loads reference datasets from an csv file") {

    val datasets = CSVReferenceDataset.fromFile(
      spark = spark,
      srcFileInputStream = this.getClass
        .getClassLoader
        .getResourceAsStream("referenceDatasets.csv"),
      datasetIDColumnNumber = 3,
      valueColumnNumber = 5,
      header = true
    )

    datasets.foreach{ case (key, dataset) =>
      println(s"$key -> ${dataset.df.collect().mkString("Array(", ", ", ")")}")
    }

    assert(datasets.keys.toList.size === 3)
    assert(datasets.keys.toList.contains("DS1"))
    assert(datasets.keys.toList.contains("DS2"))
    assert(datasets.keys.toList.contains("DS3"))

    val ds1 = datasets("DS1").df.collect().map{ row => row.getString(0) }
    val ds2 = datasets("DS2").df.collect().map{ row => row.getString(0) }
    val ds3 = datasets("DS3").df.collect().map{ row => row.getString(0) }

    assert(ds1.length === 4)
    assert(ds2.length === 4)
    assert(ds3.length === 3)

    assert(ds1 === Array("A", "B", "C", "D"))
    assert(ds2 === Array("A","E", "F", "G"))
    assert(ds3 === Array("H", "I", "J"))
  }

}
