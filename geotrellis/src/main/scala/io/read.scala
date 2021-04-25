package io


import geotrellis.spark.{LayerId, SpatialKey}
import geotrellis.spark.io.file.cog.FileCOGValueReader
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._

import java.io.File

object read {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("hdfsBasePath","hdfs://namenode:8020")
        .set("spark-master","10.101.241.5")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max","256m")
        .setIfMissing("spark.kryoserializer.buffer","64m")
        .setIfMissing("spark.driver.maxResultSize","4g")
    val sc = new SparkContext(conf)
    val catalogPath = FileAttributeStore("data/SRTM_W_250mfocalmax.tif")
    val fileValueReader = FileCOGValueReader(catalogPath)
    val key = SpatialKey(0,1)
  }
}
