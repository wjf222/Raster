package demo

import astraea.spark.rasterframes._
import org.apache.spark.sql._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster._

object ForestDemo {
  def main(args: Array[String]): Unit = {
    // withRasterFrames 允许使用rasterframes 接口
    implicit val spark = SparkSession.builder().
      master("local[*]").appName(getClass.getName).getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(s"$name")

    val filePath = "data/brazil_1/band2.tif"
    val initRF = readTiff(filePath).projectedRaster.toRF(s"band_2")
    initRF.show()
  }
}
