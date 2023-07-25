package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.rule.ArrayQueryPath

import java.util.regex.Pattern
import scala.util.control.Breaks._
import scala.util.control.Exception.allCatch
@SuppressWarnings(Array("org.wartremover.warts.Var","org.wartremover.warts.StringPlusAny","org.wartremover.warts.OptionPartial"))
case class FlattenedProfilableRecord private (id: String,
                                              flatValues: Seq[FlatValue],
                                              additionalIdentifiers: AdditionalIdentifiers) {

  def filterByPaths(pathFilters: Option[Seq[Pattern]]): FlattenedProfilableRecord =
    copy(flatValues = flatValues.filter { flatValue =>
      pathFilters.fold(true) // pathFilers is empty => always filter True and process the value
      { pathFilter =>
        // if there is a match on any of the pathfilters with this path, then filter true
        pathFilter.exists { filter => filter.matcher(flatValue.flatPath).find() }
      }
    })


  def withArrayQueryPaths(arrayQueryPath: Seq[ArrayQueryPath]): FlattenedProfilableRecord = {
      copy(flatValues =  arrayQueryPath.flatMap {queryPath =>
        val Array(arrayPath, valuePath) = queryPath.lookupPath split("\\[\\].", 2)
        ArrayFinder(arrayPath = arrayPath,
          valuePath = queryPath.finalPath,
          lookupPath = queryPath.lookupPath,
          getArray = getArray(arrayPath))
          .findValueWithQueryPath
      })
  }

  def withValueDerivedArrayIdentifier(arrayPath: String,
                                      valuePath: String,
                                      lookupPath: String,
                                      lookupValue: String,
                                      identifierName: String): FlattenedProfilableRecord = {
    copy(additionalIdentifiers = AdditionalIdentifiers(
      this.additionalIdentifiers.values ++ ArrayFinder(arrayPath = arrayPath,
      valuePath = valuePath,
      lookupPath = lookupPath,
      lookupValue = lookupValue,
      getArray = getArray(arrayPath)).findValueAsIdentifier(identifierName = identifierName)))
  }

  def getArray(path: String): Array[Seq[FlatValue]] = {

    val arrayPaths = flatValues.filter{ _.fullyQualifiedPath.startsWith(path) }
    if (arrayPaths.isEmpty) {
      // there is no matching array with this path.
      Array.empty[Seq[FlatValue]]
    } else {
      var highestIndexOpt: Option[Int] = None
      val indexedValues = arrayPaths.flatMap{ fv =>
        val cursor = path.length
        var inArrayIndex = false
        var arrayIndexStr = ""
        breakable {
          for (i <- cursor until fv.fullyQualifiedPath.length) {
            val c = fv.fullyQualifiedPath(i)
            if (c == '[') {
              inArrayIndex = true
            } else if (c == ']') {
              break
            } else if (inArrayIndex) {
              arrayIndexStr += c
            }
          }
        }

        val index = allCatch.opt(arrayIndexStr.toInt)
        index.map { i =>
          if (highestIndexOpt.isEmpty) {
            highestIndexOpt = Option(i)
          } else {
            if (i > highestIndexOpt.get) highestIndexOpt = Option(i)
          }
          i -> fv
        }
      }

      val groupedValues = indexedValues.groupBy(_._1)

      highestIndexOpt.fold(Array.empty[Seq[FlatValue]]) { highestIndex =>
        val arr = Array.ofDim[Seq[FlatValue]](highestIndex+1)
        groupedValues.foreach{ case(index, records) =>
          arr(index) = records.map{ _._2 }
        }
        arr
      }
    }
  }

  def withAdditionalIdentifiers(additionalIdentifiers: AdditionalIdentifiers): FlattenedProfilableRecord =
    copy(additionalIdentifiers = additionalIdentifiers)
}
object FlattenedProfilableRecord {
  def apply(record: ProfilableRecord, flattener: RecordFlattener): FlattenedProfilableRecord =
    flattener.flatten(record)
}