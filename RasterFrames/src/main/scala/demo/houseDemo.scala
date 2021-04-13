package demo

import org.apache.spark.sql._
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.geotiff.DataFrameReaderHasGeoTiffFormat
import geotrellis.raster.io.geotiff._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.functions.lit

import java.net.URI

object houseDemo {
  def getabsolution(path: String): String = new java.io.File(path).getAbsolutePath

  def getURI(path: String): URI = new URI(s"file:/$path")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder().
      master("local[*]").appName(getClass.getName).getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val bandnums = (1 to 4)

    def readTiff(name: String): RasterFrame = spark.read.geotiff.loadRF(getURI(getabsolution(name)))

//    readTiff(s"data/2012.tif").projectedRaster
//    val RFs = (2 to 6 by 2).map{x => readTiff(s"data/201$x.tif").projectedRaster
//      .toRF(221,225).withColumn("year", lit(s"201$x"))}
  }
}
