package uk.gov.ipt.das.dataprofiler.feature

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.feature.map.{MapNode, StringMapNode}
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers}
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf

class FeatureCollectionJSONEncoderTest extends AnyFunSpec with SparkSessionTestWrapper {

  def makeFp(id: String, path: String, value: String, featureName: String = "ORIGINALVALUE"): FeaturePoint =
    FeaturePoint(
      recordId = s"RECORD$id",
      path = path,
      originalValue = s"RECORD$id",
      feature = FeatureOutput(
        feature = FeatureDefinition(`type` = "DQ", name = featureName),
        value = StringValue(value)
      ),
      recordSet = "testSet",
      additionalIdentifiers = AdditionalIdentifiers(
        values = Seq(
          AdditionalIdentifier(name = "identifier1", value = "someDimensionalValue1"),
          AdditionalIdentifier(name = "identifier2", value = "someDimensionalValue2"),
          AdditionalIdentifier(name = "identifier3", value = "someDimensionalValue3"),
          AdditionalIdentifier(name = "identifier4", value = "someDimensionalValue4"),
          AdditionalIdentifier(name = "identifier5", value = "someDimensionalValue5"),
        )
      )
    )

  val fps1 = Seq(
    makeFp("1", "id", "RECORD1"),
    makeFp("1", "person.firstname", "John"),
    makeFp("1", "person.lastname", "Smith"),
    makeFp("1", "address.firstline", "130 Jermyn Street"),
    makeFp("1", "address.city", "London")
  )

  val fps2 = Seq(
    makeFp("2", "id", "RECORD2"),
    makeFp("2", "person.firstname", "David"),
    makeFp("2", "person.lastname", "Jones"),
    makeFp("2", "address.firstline", "22a St James Square"),
    makeFp("2", "address.city", "London")
  )

  val fps3 = Seq(
    makeFp("3", "id", "RECORD3"),
    makeFp("3", "person.firstname", "Shawn"),
    makeFp("3", "person.lastname", "Carter"),
    makeFp("3", "address.firstline", "560 State Street"),
    makeFp("3", "address.city", "New York City")
  )

  val fps4 = Seq(
    makeFp("4", "id", "RECORD3"),
    makeFp("4", "person.firstname", "Shawn"),
    makeFp("4", "person.lastname", "Carter"),
    makeFp("4", "address.firstline", "560 State Street"),
    makeFp("4", "address.firstline", "999 AAAAA AAAAA", featureName = "HIGHGRAIN"),
    makeFp("4", "address.firstline", "9 A A", featureName = "LOWGRAIN"),
    makeFp("4", "address.firstline", "TRUE", featureName = "VALID_ADDRESS"),
    makeFp("4", "address.city", "New York City")
  )

  def printMapNode(node: MapNode, indent: Int = 1): Unit = {
    if (node.children == null) {
      node.leafValues.foreach{ leafValue =>
        println(s"${"  " * indent}${leafValue.toString}")
      }
    } else {
      node.children.foreach { case (key, value) =>
        if (value.leafValues != null) {
          value.leafValues.foreach { leafValue =>
            println(s"${"  " * indent}$key: ${leafValue.toString}")
          }
        } else {
          println(s"${"  " * indent}$key: MapNode")
          printMapNode(value, indent + 1)
        }
      }
    }
  }

  def printStringMapNode(node: StringMapNode, indent: Int = 1): Unit = {
    if (node.children == null) {
      println(s"${"  " * indent}${node.leafValue}")
    } else {
      node.children.foreach { case (key, value) =>

        if (value.leafValue != null) {
          println(s"${"  " * indent}$key: ${value.leafValue}")
        } else {
          println(s"${"  " * indent}$key: StringMapNode")
          printStringMapNode(value, indent + 1)
        }
      }
    }
  }

  def printMap(node: Map[String, Any], indent: Int = 1): Unit = {
    node.foreach { case (key, value) =>

      value match {
        case points: List[FeaturePoint @unchecked] =>
          points.foreach { point => println(s"${"  " * indent}$key: ${point.toString}") }
        case subMap: Map[String @unchecked, Any @unchecked] =>
          println(s"${"  " * indent}$key: Map")
          printMap(subMap, indent + 1)
        case _ =>
          throw new Exception(s"Non FeaturePoint or Map encountered: ${value.getClass}")
      }
    }
  }

  def printPlainMap(node: Map[String, Any], indent: Int = 1): Unit = {
    node.foreach { case (key, value) =>

      value match {
        case point: String =>
          println(s"${"  " * indent}$key: $point")
        case subMap: Map[String @unchecked, Any @unchecked] =>
          println(s"${"  " * indent}$key: Map")
          printPlainMap(subMap, indent + 1)
        case _ =>
          throw new Exception(s"Non String or Map encountered: ${value.getClass}")
      }
    }
  }

  it("makes JSON strings from a FeatureCollection") {
    val fc1 = FeatureCollection(spark.createDataset(fps1))
    val fc2 = FeatureCollection(spark.createDataset(fps2))
    val fc3 = FeatureCollection(spark.createDataset(fps3))
    val fc4 = FeatureCollection(spark.createDataset(fps4))

    val json1 = FeatureCollectionJSONEncoder.toJSONStrings(fc1, pretty = true)
    val json2 = FeatureCollectionJSONEncoder.toJSONStrings(fc2, pretty = true)
    val json3 = FeatureCollectionJSONEncoder.toJSONStrings(fc3, pretty = true)
    val json4 = FeatureCollectionJSONEncoder.toJSONStrings(fc4, pretty = true)

    json1.collect().foreach{ println }
    json2.collect().foreach{ println }
    json3.collect().foreach{ println }
    json4.collect().foreach{ println }

    val all = FeatureCollection(Seq(fc1, fc2, fc3, fc4))
    val allJson = FeatureCollectionJSONEncoder.toJSONStrings(all, pretty = true)

    allJson.collect().foreach{ println }
  }

}
