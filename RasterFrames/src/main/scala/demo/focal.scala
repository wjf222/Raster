package demo

import astraea.spark.rasterframes.datasource.geotiff._
import astraea.spark.rasterframes.{RasterFrame, WithSparkSessionMethods}
import astraea.spark.rasterframes.functions._
import geotrellis.raster.{DoubleConstantNoDataCellType, IntConstantNoDataCellType, Tile}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import java.net.URI

object focal {
  def main(args: Array[String]): Unit = {
    val maskedPath = "hdfs://namenode:8020" + args(0)
    val resultPath = "hdfs://namenode:8020" + args(1)
    val sparkconf: SparkConf = new SparkConf().setAppName("RasterFrames")
      //      .setMaster("local[*]")
      .set("spark-master", "10.101.241.5")
    implicit val spark = SparkSession.builder().
      config(sparkconf).getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")

//    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    def readTiff(name: String): RasterFrame = spark.read.geotiff.loadRF(new URI(s"$name"))

    val localGreater = udf((band: Tile) ⇒ {
        val bandd = band.convert(IntConstantNoDataCellType)
        bandd.localGreater(1)
    })

    val filePath = "file:/D:/mergeFile/B02-RM-RN-tiled.tif"
    val initRF = readTiff(maskedPath)
    for (i <- 0 to initRF.columns.length-1)
      println(initRF.columns(i))
//    val tmp = initRF.withColumn("local", localGreater($"tile")).asRF
//    tmp.write.save(resultPath)
  }
}
