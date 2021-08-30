package com.wyb.spark.lineage.localHdfs

import java.io.File

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}

object TestMiniHdfs extends EmbeddedHdfsSpark {
  def main(args: Array[String]): Unit = {

    if (SystemUtils.IS_OS_WINDOWS)
    // on Windows, use a tmp folder without spaces
      startHdfs(new File("c:\\tmp\\hdfs"))
    else
      startHdfs()
    copyFromLocal(
      "data/in/lineagecap/insurance_sample.csv.gz",
      "/insurance_sample.csv.gz"
    )
    copyFromLocal("data/in/lineagecap/characters.json", "/characters.json")
    copyFromLocal("data/in/lineagecap/locations.json", "/locations.json")

    fileSystem.delete(new Path("/"),true)

    val ss: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(new Path("/"),false)
    while (ss.hasNext) {
      println("ls: " + ss.next().toString)
    }

    stopHdfs()
  }

}
