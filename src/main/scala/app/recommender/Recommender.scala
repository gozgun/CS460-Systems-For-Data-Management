package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similarMovies = nn_lookup.lookup(sc.parallelize(Seq(genre)))
      .flatMap { case (_, movies) => movies.map(x => x._1) } // extract the movie id
      .collect()

    val userRatings = ratings.filter(x => x._1 == userId) // movies rated by user and their ratings
      .map(x => x._2)
      .collect()

    // Filter out movies already rated by user and get predictions for remaining movies
    val predictions = similarMovies
      .flatMap(movieId => {
        val predictedRating = baselinePredictor.predict(userId, movieId)
        if (userRatings.contains(movieId)) { // if the user has already rated it, skip
          None
        } else {
          Some((movieId, predictedRating))
        }
      })
      .sortBy(-_._2) // descending order of predictedRating
      .take(K)

    predictions.toList
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similarMovies = nn_lookup.lookup(sc.parallelize(Seq(genre)))
      .flatMap { case (_, movies) => movies.map(x => x._1) }
      .collect()

    val userRatings = ratings.filter(x => x._1 == userId)
      .map(x => x._2)
      .collect()

    // Filter out movies already rated by user and get predictions for remaining movies
    val predictions = similarMovies
      .flatMap(movieId => {
        val predictedRating = collaborativePredictor.predict(userId, movieId)
        if (userRatings.contains(movieId)) { // if the user has already rated it, skip
          None
        } else {
          Some((movieId, predictedRating))
        }
      })
      .sortBy(-_._2) // descending order of predictedRating
      .take(K)

    predictions.toList
  }
}
