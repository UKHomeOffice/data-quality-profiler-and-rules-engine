package uk.gov.ipt.das.dataprofiler.wrapper

import java.io.File
import java.nio.file.Files

trait FileOutputWrapper {

  def tmpDir(prefix: String = "test_", create: Boolean = true): String = {
    val newDir = Files.createTempDirectory(prefix)
    if (!create) {
      Files.delete(newDir)
    }
    newDir.toFile.getAbsolutePath
  }

  def deleteFile(path: String): Unit = {
    val fileTemp = new File(path)
    if (fileTemp.exists) {
      fileTemp.delete()
    }
  }

}
