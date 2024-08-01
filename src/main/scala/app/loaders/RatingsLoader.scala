package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    var rddInput = sc.textFile(new File(getClass.getResource(path).getFile).getPath)
    val rddData = rddInput.map(line => {
      val fields = line.split('|')
      val userID = fields(0).toInt // user id
      val movieID = fields(1).toInt // movie id
      val rating = fields(2).toDouble // rating
      val timestamp = fields(3).toInt // timestamp
      (userID, movieID, Option.empty[Double], rating, timestamp)
    })
    rddData.persist(StorageLevel.MEMORY_ONLY) // cache
  }
}