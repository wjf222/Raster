package io


import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import com.typesafe.config.ConfigFactory
/**
 * @author xuanfenwang
 */
object MaskBandsRandGandNIR {
  val maskedPath = "D:/mergeFile/r-g-nir-RPP.tif"
  //constants to differentiate which bands to use
  val R_BAND = 0
  val NIR_BAND = 1
  def bandPath(b: String) = s"D:/mergeFile/${b}.tif"

  def main(args: Array[String]): Unit = {
    // Read in the red band
    println("Reading in the red band...")
    val rGeoTiff = SinglebandGeoTiff(bandPath("B02-RM"))

    // Read in the near infrared band
    println("Reading in the NIR band...")
    val nirGeoTiff = SinglebandGeoTiff(bandPath("NIR-RM"))

    // Mask our red, green and near infrared bands using the qa band
    println("Masking clouds in the red band...")
    val rMasked = rGeoTiff.tile
    println("Masking clouds in the NIR band...")
    val nirMasked = nirGeoTiff.tile

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rMasked, nirMasked).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + NIR tile...")
    MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).write(maskedPath)
  }
}
