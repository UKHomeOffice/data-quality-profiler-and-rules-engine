package uk.gov.ipt.das.dataprofiler.reporting.template.mixin

import org.apache.commons.io.IOUtils

import java.io.InputStream
import java.util.Base64

trait ImageToDataURL {

  def imageToDataURL(inputStream: InputStream, typeString: String): String =
    s"data:image/$typeString;base64,${Base64.getEncoder.encodeToString(IOUtils.toByteArray(inputStream))}"

  def imageToDataURL(bytes: Array[Byte], typeString: String): String =
    s"data:image/$typeString;base64,${Base64.getEncoder.encodeToString(bytes)}"

}
