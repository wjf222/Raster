package io

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, Tile}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.compression.NoCompression
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.singlebandGeoTiffReader
import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster.render.Png
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.cog.COGLayer
import geotrellis.spark.io.file.FileAttributeStore
import geotrellis.spark.io.file.cog.FileCOGLayerWriter
import geotrellis.spark.{Metadata, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata, TileLayerRDD, withTilerMethods}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.render.withSpatialTileRDDRenderMethods
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.vector.ProjectedExtent
import io.IngestImage.inputPath
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object cacul {
  val maskedPath = "data/SRTM_W_250m.tif"
  val resultPath = "data/SRTM_W_250mfocalmax.tif"
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
//    val hdfsBasePath:String =
    val sc = new SparkContext(conf)
    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(maskedPath)
    }
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))
    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    }
    val result = tiled.mapValues { tile =>
      tile.localGreater(1)
    }
    val layoutScheme = ZoomedLayoutScheme(WebMercator,tileSize = 2048)
    val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(result, rasterMetaData)
        .reproject(WebMercator, layoutScheme, Bilinear)
    val attributeStore = FileAttributeStore(resultPath)
    val writer = FileCOGLayerWriter(attributeStore)
    val layerName = "layername"
    val  cogLayer = COGLayer.fromLayerRDD(reprojected,zoom)
    val keyIndex = cogLayer.metadata.zoomRangeInfos.map{
      case (zr,bounds) => zr -> ZCurveKeyIndexMethod.createIndex(bounds)
    }.toMap
    writer.writeCOGLayer(layerName,cogLayer,keyIndex)
    print(1)
  }
}
