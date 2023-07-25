package uk.gov.ipt.das.dataprofiler.feature

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.map.MapNode.ADDITIONAL_IDENTIFIERS_MAPNODE_FIELD
import uk.gov.ipt.das.dataprofiler.feature.map.{MapNode, StringMapNode}

import scala.collection.mutable
@SuppressWarnings(Array("org.wartremover.warts.IterableOps", "org.wartremover.warts.Product","org.wartremover.warts.Serializable",
                        "org.wartremover.warts.Null","org.wartremover.warts.NonUnitStatements"))
object FeatureCollectionJSONEncoder {

  def toJSONStrings(featureCollection: FeatureCollection, pretty: Boolean = false): Dataset[(String, String)] = {
    import featureCollection.featurePoints.sparkSession.implicits._

    featureCollection
      .featurePoints
      .groupByKey{ fp => fp.recordId }
      .mapGroups { case (recordId: String, featurePoints: Iterator[FeaturePoint]) =>

        val fps = featurePoints.toList

        /** This aiNode is a top-level key to add additional identifiers like so:
         *
         * {
         *    "_additional_identifiers": {
         *       "identifierName1": "value",
         *       "identifierName2": "value2"
         *    },
         *    ... record ...
         * }
         *
         */

        val aiNode = StringMapNode.children(
          fps.headOption.fold(Seq[(String, StringMapNode)]()){ headFeaturePoint =>
            headFeaturePoint.additionalIdentifiers.values.map { identifier =>
              identifier.name -> StringMapNode.leaf(identifier.value)
            }
          }
        )

        val stringMapNode = groupLevel(
          parentMap = MapNode.root,
          outerLevel =
            fps.map { featurePoint =>
              val (start, rest) = nextPathLevelKey(featurePoint.path)
              (start, rest, featurePoint)
            }
        ).asStringMapNode

        stringMapNode.children.put(ADDITIONAL_IDENTIFIERS_MAPNODE_FIELD, aiNode)

        recordId -> stringMapNode
          .asJSONString(pretty = pretty)
      }
  }

  private def nextPathLevelKey(path: String): (String, String) = {
    val splitPath = path.split("[.]", 2)

    if (splitPath.length == 1) {
      (splitPath(0), null)
    } else {
      (splitPath(0), splitPath(1))
    }
  }

  private def groupLevel(parentMap: MapNode, outerLevel: List[(String, String, FeaturePoint)]): MapNode = {
    outerLevel
      .groupBy { case (start: String, _, _) => start }
      .foreach { case (thisStart: String, level: List[(String, String, FeaturePoint)]) =>

        if (level.head._2 == null) {
          // leaf nodes
          parentMap.children.put(level.head._1, MapNode(null, level.map { _._3 }))
        } else {
          val thisLevelMap = MapNode(mutable.HashMap[String, MapNode](), null)
          parentMap.children.put(thisStart, thisLevelMap)

          level
            .map {
              case (_: String, rest: String, featurePoint: FeaturePoint) =>
                val (nextStart, nextRest) = nextPathLevelKey(rest)
                (nextStart, nextRest, featurePoint)
            }
            .groupBy{ _._1 }
            .map { case (nextStart: String, points: List[(String, String, FeaturePoint)]) =>
              if (points.head._2 == null) {
                thisLevelMap.children.put(nextStart, MapNode(null, points.map {
                  _._3
                }))
              } else {
                groupLevel(thisLevelMap, points)
              }
            }
        }
      }
    parentMap
  }

}
