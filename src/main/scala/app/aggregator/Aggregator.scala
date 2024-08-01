package app.aggregator

import com.esotericsoftware.kryo.serializers.FieldSerializer.Optional
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null
  private var averageRDD: RDD[(Int, String, List[String], Option[Double], Int, Double, Double)] = null
  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    var id_rating = ratings.map(r => (r._2, r)) // movie id, rating info
    var id_title = title.map(t => (t._1, t)) // movie id, movie info

    var mergedRDD = id_rating.rightOuterJoin(id_title).map(x => (x._1, x._2._2._2, x._2._2._3, x._2._1 match {
      case Some(x) => x._4 // ratings
      case None => 0 // no ratings
    })) // final RDD: movie id, movie title, keywords, ratings or O if unrated

    var groupRDD = mergedRDD.map(r => ((r._1, r._2, r._3),(1, r._4))) // ((movie id, movie title, keywords) , (1, rating))
    var sumRDD = groupRDD.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    // (movie id, movie title, keywords, count, sum, avg rating)
    averageRDD = sumRDD.map(x => (x._1._1, x._1._2, x._1._3, None.asInstanceOf[Option[Double]], x._2._1, x._2._2, x._2._2 / x._2._1 ))
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    averageRDD.map(x => (x._2, x._7))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // filter titles which has the keywords
    var filteredRDD = averageRDD.filter(r => keywords.forall(keyword => r._3.contains(keyword)))
    if (filteredRDD.count() == 0) { // no titles found, return -1
        -1.0
    }
    else {
      val sumVotes = filteredRDD.count() // sum of number of votes
      val sumAvgRatings = filteredRDD.map(x => x._7).reduce(_ + _) // sum of average ratings of the movies
      val res = sumAvgRatings / sumVotes // average of movies with the given keywords
      if (res == 0) {
        0.0
      } else {
        res
      }
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    var indexRDD = averageRDD.map(x => (x._1, x)) // movie id and RDD
    var updateRDD = sc.parallelize(delta_).map(x =>
        x._3 match {
          // there is an old rating, remove it and add the new one to the sum
          case Some(y) => (x._2, (x._2, "", List[String](), None.asInstanceOf[Option[Double]], 0, x._4 - y, 0.0))
          // no rating before
          case None => (x._2, (x._2, "", List[String](),None.asInstanceOf[Option[Double]], 1, x._4, 0.0))
      }
    )
    // combine the indexRDD and updateRDD with movie id
    var concat = indexRDD.union(updateRDD)
    // reduce by movie id and compute new count, sum, and rating for each movie
    var newRDD = concat.reduceByKey((rhs, lhs) => (rhs._1, rhs._2, rhs._3, rhs._4, rhs._5 + lhs._5, rhs._6 + lhs._6, (rhs._6 + lhs._6) / (rhs._5 + lhs._5)))
      .map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)))
    // id, (id, title, keywords, optional, count, sum, avg)

    // delete the old RDD and save the new one
    averageRDD.unpersist()
    newRDD.persist(StorageLevel.MEMORY_ONLY)
    averageRDD = newRDD.map(x => (x._1, x._2._2, x._2._3, None.asInstanceOf[Option[Double]], x._2._5, x._2._6, x._2._7))
    // (movie id, movie title, keywords, optional, count, sum, avg rating)
  }
}

