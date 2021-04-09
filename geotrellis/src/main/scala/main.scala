
import geotrellis.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object main extends App {
  val conf = new SparkConf()
    .setAppName("Geo")
    .setIfMissing("spark.master", "local[*]")
  implicit val spark = SparkSession.builder().config(conf).getOrCreate()
  implicit val sc = spark.sparkContext
  val FilePath: String = "arg_wn/"
  var outPngPath: String = ""
  val catalogPath: String = "" /* Some location on your computer */

  var LayerName: String = "myLayer"
  var layerId = LayerId("mylayer2", 2)
  var Zoom: Int = 3
//  val store: AttributeStore = FileAttributeStore(catalogPath)
//
//  val reader = FileLayerReader(FilePath)
//  val writer = FileLayerWriter(FilePath)
//  val sLayer:TileLayerRDD[SpatialKey] = reader.read(layerId)
  print(1)
  def ReadOrWrite(): Unit = {
    val StoreType: String = "FILE"
    val FileType: String = "COG"
    val read = false
    LayerName = StoreType + "_" + "FileType"
    layerId = LayerId(LayerName, Zoom)
    outPngPath = FilePath + "\\" + layerId.name + ".png"

  }

}
