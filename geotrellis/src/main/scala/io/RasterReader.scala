package io

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xuanfenwang
 */
trait RasterReader {
  def read(path:String)
}
