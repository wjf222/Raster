package convert

import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata, TileLayerRDD, withCollectMetadataMethods, withTilerMethods}
import geotrellis.spark.io.AttributeStore.Fields.metadata
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.{FloatingLayoutScheme, Tiler}
import geotrellis.vector.{Extent, ProjectedExtent}
import io.IngestImage.inputPath
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import geotrellis.raster.io.geotiff.GeoTiff

object RDDToTile {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(inputPath)
    val layoutScheme: FloatingLayoutScheme = FloatingLayoutScheme(512)

    val (_:Int,metadata:TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](layoutScheme)

    val tileOptions = Tiler.Options(
      resampleMethod = Bilinear, partitioner = Option(new HashPartitioner(inputRdd.partitions.length))
    )

    val tileRdd = inputRdd.tileToLayout[SpatialKey](metadata,tileOptions)
    val layerRdd: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(tileRdd,metadata)


    val raster: Raster[MultibandTile] = layerRdd.filter().result.stitch()

    GeoTiff(raster,metadata.crs).write("data/geotrellis/result.tif")
  }



}
