package io

import geotrellis.proj4.LatLng
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, Tile}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{SpatialKey, TileLayerMetadata, withTilerMethods}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.render.withSpatialTileRDDRenderMethods
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.ProjectedExtent
import io.IngestImage.inputPath
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object cacul {
  val maskedPath = "data/SRTM_W_250m.tif"
  val resultPath = "data/SRTM_W_250mfocalmax.png"
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max","256m")
        .setIfMissing("spark.kryoserializer.buffer","64m")
        .setIfMissing("spark.driver.maxResultSize","4g")
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
    tiled.mapValues { tile =>
        tile.focalMax(Square(3))
    }.foreach(ST => ST._2.renderPng().write(resultPath))
    print(1)
  }
}
