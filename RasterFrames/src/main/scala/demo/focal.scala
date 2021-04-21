package demo

import astraea.spark.rasterframes.{WithProjectedRasterMethods, WithSparkSessionMethods}
import astraea.spark.rasterframes.functions._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql.SparkSession

object focal {
  def main(args: Array[String]): Unit = {
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
