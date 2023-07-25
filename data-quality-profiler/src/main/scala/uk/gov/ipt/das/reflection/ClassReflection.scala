package uk.gov.ipt.das.reflection

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
trait ClassReflection {

  def classesInPackage[T](pkg: String, classLoader: ClassLoader = ClassLoader.getSystemClassLoader): Seq[T] =
    new BufferedReader(new InputStreamReader(
      classLoader.getResourceAsStream(pkg.replaceAll("[.]", "/"))))
      .lines.iterator().asScala.toList
      .filter { line => line.endsWith(".class") && !line.endsWith("$.class") }
      .map { line =>
        val cls = Class.forName(s"$pkg.${line.substring(0, line.lastIndexOf('.'))}")
        cls.getMethod("apply")
          .invoke(cls)
          .asInstanceOf[T]
      }

  def instanceByClassName[T](name: String, pkg: String = ""): T = {
    val fullName = if (pkg == "") name else s"$pkg.$name"
    val cls = Class.forName(fullName)
    cls.getMethod("apply")
      .invoke(cls)
      .asInstanceOf[T]
  }

}
