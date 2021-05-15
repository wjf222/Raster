package demo

import astraea.spark.rasterframes.{WithSparkSessionMethods, _}
import astraea.spark.rasterframes.datasource.geotiff.DataFrameReaderHasGeoTiffFormat
import geotrellis.raster._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import java.net.URI

object local {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("RasterFrames")
//      .setMaster("local[*]")
      .set("spark-master", "10.101.241.5")
    implicit val spark = SparkSession.builder().
      config(sparkconf).getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")
//    val config = ConfigFactory.load()
    val hdfsBasePath:String = "hdfs://namenode:8020"
    val red_url = hdfsBasePath + args(0)
    val output_url = hdfsBasePath + args(1)
//    val output_url = "file:/C:/Users/DELL/IdeaProjects/Raster/data/brazil_1/band5-nvdi.tif"
    import spark.implicits._

    spark.read
    def redBand = spark.read.geotiff.loadRF(new URI(red_url)).withColumnRenamed("tile","red_band").asRF
    // Define UDF for computing NDVI from red and NIR bands
    val ndvi = udf((red: Tile) ⇒ {
      val bandd = red.convert(IntConstantNoDataCellType)
      bandd.localGreater(1)
    })

    // We use `asRF` to indicate we know the structure still conforms to RasterFrame constraints
    val r_nir_band = redBand
    val rf2 = r_nir_band.withColumn("ndvi", ndvi($"red_band")).asRF

    for (i <- 0 to rf2.columns.length-1)
      println(rf2.columns(i))
    rf2.write.save(output_url)
//    val pr = rf2.toRaster($"ndvi", 1098, 1098)
//
//    val brownToGreen = ColorRamp(
//      RGBA(166,97,26,255),
//      RGBA(223,194,125,255),
//      RGBA(245,245,245,255),
//      RGBA(128,205,193,255),
//      RGBA(1,133,113,255)
//    ).stops(128)
//
//    val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
//    // change writing location
//    pr.tile.color(colors).renderPng().write(output_url)
    //For a georefrenced singleband greyscale image, could do: `GeoTiff(pr).write("ndvi.tiff")`
  }
}