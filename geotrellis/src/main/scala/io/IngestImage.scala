package io

import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.ColorMap
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import geotrellis.util.withGetComponentMethods
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File

object IngestImage {
  //  val inputPath = "file://" + new File("data/arg_wm/DevelopedLand.tiff").getAbsolutePath
  val inputPath = "data/SRTM_W_250m.tif"
  val outputPath = "data/catalog"

  //  val outputPath = ""
  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  def run(implicit sc: SparkContext) = {
    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] = {
    sc.hadoopMultibandGeoTiffRDD(inputPath)
    }

    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    val (_, rasterMetaData) =
    TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiled: RDD[(SpatialKey, MultibandTile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    }
    val dont = tiled.mapValues { tile =>
      tile.convert(DoubleConstantNoDataCellType).combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND) { (r: Double, ir: Double) =>
        Calculations.ndvi(r, ir);
      }
    }
      .map { case (key, tile) => (key.getComponent[SpatialKey], tile) }
      .reduceByKey(_.localMax(_)).stitch()
    val colorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.ndviColormap")).get

    dont.renderPng(colorMap).write("data/geeotrellis/nvdi.png")
//    val raster: Raster[Tile] = Raster(dont)
//    GeoTiff(dont, rasterMetaData.crs).write("data/geeotrellis/nvdi.png")
    print(dont)
    // We'll be tiling the images using a zoomed layout scheme
    // in the web mercator format (which fits the slippy map tile specification).
    // We'll be creating 256 x 256 tiles.
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // We need to reproject the tiles to WebMercator
    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiled, rasterMetaData)
        .reproject(WebMercator, layoutScheme, Bilinear)

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId("landsat", z)
      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }
}
