package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val rddInput = sc.textFile(new File(getClass.getResource(path).getFile).getPath)
    val rddData = rddInput.map(line => {
      val clean_line = line.replace("\"", "") // removes the " symbol
      val fields = clean_line.split('|')
      val id = fields(0).toInt  // movie id
      val title = fields(1)   // movie title
      val genres = fields.drop(2).toList // movie genres , slice, drop
      (id, title, genres)
    })
    rddData.persist(StorageLevel.MEMORY_ONLY) // cache
  }
}

