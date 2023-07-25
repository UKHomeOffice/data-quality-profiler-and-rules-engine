package uk.gov.ipt.das.dataprofiler.feature.map

import uk.gov.ipt.das.dataprofiler.feature.FeaturePoint
import scala.collection.mutable
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null", "org.wartremover.warts.Product","org.wartremover.warts.Null",
                        "org.wartremover.warts.Throw", "org.wartremover.warts.IterableOps", "org.wartremover.warts.Serializable"))
//TODO This data type could be re written if its anything like a binary tree.
case class MapNode(children: mutable.HashMap[String, MapNode], leafValues: Seq[FeaturePoint]) {

  def asMap: Map[String, Any] = {
    if (leafValues != null) {
      throw new Exception("Tried to turn a leaf into a map")
    }

    children.map {
      case (key: String, value: MapNode) =>
        if (value.leafValues != null) {
          key -> value.leafValues
        } else {
          key -> value.asMap
        }
    }.toMap
  }

  def asStringMapNode: StringMapNode = {
    if (leafValues != null) {
      throw new Exception("Tried to turn a leaf into a StringMapNode")
    }

    StringMapNode.children(
      children
        .groupBy{ case (key: String, _: MapNode) => key }
        .flatMap { case (key: String, grouped: mutable.HashMap[String, MapNode]) =>
          if (grouped.values.exists(_.leafValues == null) ) {
            // submap
            grouped.values.map { key -> _.asStringMapNode }
          } else {
            val originalValue = grouped.values.head.leafValues.head.originalValue
            val originalPath = grouped.values.head.leafValues.head.path
            // leaf
            Seq(
              key -> StringMapNode.leaf(originalValue),
              s"${key}__DQ" ->
                StringMapNode.children(
                    Seq[(String, StringMapNode)](
                      "path" -> StringMapNode.leaf(originalPath),
                      "profile_masks" -> StringMapNode.children(
                          grouped.values
                            .flatMap { _.leafValues }
                            .map { leafValue =>
                              leafValue.feature.feature.getFieldName -> StringMapNode.leaf(leafValue.feature.getValueAsString)
                            }
                        )
                    )
                )
            )
          }
        }
    )
  }
}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object MapNode {
  val ADDITIONAL_IDENTIFIERS_MAPNODE_FIELD = "_additional_identifiers"
  def root: MapNode = children(Seq())
  def leaf(values: FeaturePoint*): MapNode = MapNode(null, values)
  def children(children: Iterable[(String, MapNode)]): MapNode =
    MapNode(mutable.HashMap[String, MapNode](children.toSeq: _*), null)
}