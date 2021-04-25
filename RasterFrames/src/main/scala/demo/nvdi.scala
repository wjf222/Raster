package demo

import astraea.spark.rasterframes.{WithProjectedRasterMethods, WithSparkSessionMethods}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.render.{ColorMap, ColorRamp, RGBA}
import org.apache.spark.sql.SparkSession
import astraea.spark.rasterframes._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf

object nvdi {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("RasterFrames").setMaster("local[*]")
      .set("spark-master", "10.101.241.5")
    implicit val spark = SparkSession.builder().
      config(sparkconf).getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")
    val config = ConfigFactory.load()
    val hdfsBasePath:String = "hdfs://namenode:8020"
    print(hdfsBasePath)
    val nir_url = hdfsBasePath + config.getString("resource.nir_url")
    val red_url = hdfsBasePath + config.getString("resource.red_url")
    val output_url = hdfsBasePath + config.getString("resource.output_url")
    import spark.implicits._

    spark.read
    def redBand = SinglebandGeoTiff(red_url).projectedRaster.toRF("red_band")
    def nirBand = SinglebandGeoTiff(nir_url).projectedRaster.toRF("nir_band")

    // Define UDF for computing NDVI from red and NIR bands
    val ndvi = udf((red: Tile, nir: Tile) ⇒ {
      val redd = red.convert(DoubleConstantNoDataCellType)
      val nird = nir.convert(DoubleConstantNoDataCellType)
      (nird - redd)/(nird + redd)
    })

    // We use `asRF` to indicate we know the structure still conforms to RasterFrame constraints
    val rf2 = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asRF
    val pr = rf2.toRaster($"ndvi", 1098, 1098)

    val brownToGreen = ColorRamp(
      RGBA(166,97,26,255),
      RGBA(223,194,125,255),
      RGBA(245,245,245,255),
      RGBA(128,205,193,255),
      RGBA(1,133,113,255)
    ).stops(128)

    val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
    // change writing location
    pr.tile.color(colors).renderPng().write(output_url)
    //For a georefrenced singleband greyscale image, could do: `GeoTiff(pr).write("ndvi.tiff")`
  }
}