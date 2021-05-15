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

    initRF.select(tileStats($"band_2")).show(false)

    initRF.select(cellType($"band_2")).show()

    initRF.select(tileMean($"band_2")).show()

    // Loading the rest of the bands

    // Three scenes
    val sceneNums = 1 to 3
    // Four bands per scene (2, 3, 4, and 5)
    val bandNums = 2 to 5

    val filePattern = "data/brazil_%d/band%d.tif"

    val fullRFs = sceneNums.map{sn => bandNums.map { bn => (bn, filePattern.format(sn, bn))}
      .map{bandFile => readTiff(bandFile._2).projectedRaster.toRF("band_%d".format(bandFile._1))}
      .reduce(_ spatialJoin _)}

    for (rf <- fullRFs) rf.show()

    fullRFs.apply(0).withCenterLatLng().select("center").show(1, false)

    val ndviRFs = fullRFs.map { rf => rf.withColumn("ndvi",
      normalizedDifference(convertCellType($"band_5", "float32"), convertCellType($"band_4", "float32"))) }

    val completeRFs = ndviRFs.map { rf => rf.withColumn("grvi",
      normalizedDifference(convertCellType($"band_3", "float32"), convertCellType($"band_4", "float32"))) }

    for (rf <- completeRFs) rf.select(tileStats($"ndvi")).show(false)

    completeRFs.apply(0).select(tileStats($"ndvi")).show()
    completeRFs.apply(0).select(tileStats($"grvi")).show()
    completeRFs.apply(0).select(tileStats($"band_5")).show()

    val scaledRFs = completeRFs.map { rf => rf.withColumn("ndvi_s", localMultiplyScalar($"ndvi", 500.0))
      .withColumn("grvi_s", localMultiplyScalar($"grvi", 500.0)) }

    scaledRFs
    // PIPLINE
    import astraea.spark.rasterframes.ml.TileExploder

    val exploder = new TileExploder()

    val bandColNames = bandNums.map { x => "band_%d".format(x) }


    import org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols((bandColNames :+ ("ndvi") :+ ("grvi")).toArray)
      .setOutputCol("features")

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

    val kmeans = new KMeans().setK(3)

    // We establish our pipeline with our stages
    val pipeline = new Pipeline().setStages(Array(exploder, assembler, kmeans))
    val models = completeRFs.map { rf => pipeline.fit(rf) }

    val rfModelZip = completeRFs.zip(models)

    val clusteredDFs = rfModelZip.map { rf_md => rf_md._2.transform(rf_md._1) }

    clusteredDFs.apply(0).drop("row_index", "features").show(3)

    // Visualizing Results

    val tiledRFs = (0 to 2).map { i =>

      val tlm = fullRFs.apply(i).tileLayerMetadata.left.get

      clusteredDFs.apply(i).groupBy($"spatial_key").agg(
        assembleTile(
          $"column_index", $"row_index", $"prediction",
          tlm.tileCols, tlm.tileRows, ByteConstantNoDataCellType
        )
      ).asRF
    }

    val resolutions = Seq((229, 225), (232, 246), (235, 231))

    val zipPredsRes = tiledRFs.zip(resolutions)

    val predRasters = zipPredsRes.map { pdsRes => pdsRes._1.toRaster($"prediction", pdsRes._2._2, pdsRes._2._1) }

    import geotrellis.raster.render._

    val cmap = ColorRamp(0x0ab881FF, 0xe9aa0cFF, 0xe9e10cFF)

    val cmap_r = ColorRamp(0xe9e10cFF, 0xe9aa0cFF, 0x0ab881FF)

    val clusterColors = IndexedColorMap.fromColorMap(cmap.toColorMap((0 until 3).toArray))
    // Reverse the aboce color
    val clusterColorsReverse = IndexedColorMap.fromColorMap(cmap_r.toColorMap((0 until 3).toArray))

    predRasters.apply(0).tile.renderPng(clusterColors).write("data/_images/clust1.png")
    predRasters.apply(1).tile.renderPng(clusterColors).write("data/_images/clust2.png")
    predRasters.apply(2).tile.renderPng(clusterColorsReverse).write("data/_images/clust3.png")

  }
}
