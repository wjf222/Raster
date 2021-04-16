package demo

import astraea.spark.rasterframes.{WithProjectedRasterMethods, WithSparkSessionMethods}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.render.{ColorMap, ColorRamp, RGBA}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import astraea.spark.rasterframes._

import scala.io.StdIn

object nvdi {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder().
      master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val scene = SinglebandGeoTiff("data/samples/L8-B8-Robinson-IL.tiff")
    val rf = scene.projectedRaster.toRF(128, 128).cache()

    def redBand = SinglebandGeoTiff("data/samples/L8-B4-Elkton-VA.tiff").projectedRaster.toRF("red_band")
    def nirBand = SinglebandGeoTiff("data/samples/L8-B5-Elkton-VA.tiff").projectedRaster.toRF("nir_band")

    // Define UDF for computing NDVI from red and NIR bands
    val ndvi = udf((red: Tile, nir: Tile) ⇒ {
      val redd = red.convert(DoubleConstantNoDataCellType)
      val nird = nir.convert(DoubleConstantNoDataCellType)
      (nird - redd)/(nird + redd)
    })

    // We use `asRF` to indicate we know the structure still conforms to RasterFrame constraints
    val rf2 = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asRF

    val pr = rf2.toRaster($"ndvi", 466, 428)

    val brownToGreen = ColorRamp(
      RGBA(166,97,26,255),
      RGBA(223,194,125,255),
      RGBA(245,245,245,255),
      RGBA(128,205,193,255),
      RGBA(1,133,113,255)
    ).stops(128)

    val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
    // change writing location
    pr.tile.color(colors).renderPng().write("data/outputs/rf-ndvi.png")
    StdIn.readLine()
    //For a georefrenced singleband greyscale image, could do: `GeoTiff(pr).write("ndvi.tiff")`
  }
}
