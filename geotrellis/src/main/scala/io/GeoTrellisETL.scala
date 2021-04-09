package io

import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import geotrellis.spark.util.SparkUtils

object GeoTrellisETL {
//  type I = ProjectedExtent // or TemporalProjectedExtent for temporal ingest
//  type K = SpatialKey // or SpaceTimeKey for temporal ingest
//  type V = Tile // or MultibandTile to ingest multiband tile
//  def main(args: Array[String]): Unit = {
//    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
//    try {
//      EtlConf(args) foreach { conf =>
//        /* parse command line arguments */
//        val etl = Etl(conf)
//        /* load source tiles using input module specified */
//        val sourceTiles = etl.load[I, V]
//        /* perform the reprojection and mosaicing step to fit tiles to LayoutScheme specified */
//        val (zoom, tiled) = etl.tile[I, V, K](sourceTiles)
//        /* save and optionally pyramid the mosaiced layer */
//        etl.save[K, V](LayerId(etl.input.name, zoom), tiled)
//      }
//    } finally {
//      sc.stop()
//    }
//  }
}