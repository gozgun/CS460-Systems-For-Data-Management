package app.recommender.baseline

import breeze.linalg.InjectNumericOps
import org.apache.spark.rdd.RDD

import scala.collection.parallel.immutable
import scala.collection.Map


class BaselinePredictor() extends Serializable {
  private var state = null
  private var globalAvgRating: Double = _
  private var userNormalizedDeviationsMap: Map[Int, Map[Int, Double]] = null
  private var globalAvgDeviationsMap: Map[Int, Double] = null
  private var userAvgRatings:  Map[Int, Double] = null

  private def scale(x: Double, avg: Double): Double = {
    if(x > avg){
      5 - avg
    }
    else if(x < avg){
      avg - 1
    }
    else{ // x = avg
      1
    }
  }

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Creating Map => user id (movie id, global average deviation)
    val userNormalizedDeviationsRDD = ratingsRDD.map { case (userId, movieId, _, rating, _) =>
      (userId, (movieId, rating))
    }.groupByKey().flatMap { case (userId, ratings) =>
      val userAvgRating = ratings.map(_._2).sum / ratings.size.toDouble
      ratings.flatMap { case (movieId, rating) =>
        val deviation = (rating - userAvgRating) / scale(rating, userAvgRating)
        Seq((userId, movieId, deviation))
      }
    }
    userNormalizedDeviationsMap = userNormalizedDeviationsRDD.map { case (userId, movieId, deviation) =>
      (userId, (movieId, deviation))
    }.groupByKey().mapValues(_.toMap).collectAsMap()

    // Creating Map => movie id, global average deviation
    globalAvgDeviationsMap = userNormalizedDeviationsRDD.map { case (_, movieId, deviation) =>
      (movieId, deviation)
    }.groupByKey().mapValues { deviations =>
      if (deviations.isEmpty) 0.0
      else deviations.sum / deviations.size.toDouble
    }.collectAsMap()


    // Users' average rating for all movies, r_u_dot
    val userRatings = ratingsRDD.map { case (userId,_, _, rating,_) =>
      (userId, (rating, 1))
    }
    userAvgRatings = userRatings.reduceByKey { case ((rating1, count1), (rating2, count2)) =>
      (rating1 + rating2, count1 + count2)
    }.mapValues { case (ratingSum, count) =>
      ratingSum / count
    }.collectAsMap()

    // Calculate the global average rating across all users and all movies
    val ratingsCount = ratingsRDD.count()
    val ratingsTotal = ratingsRDD
      .map{ case (_, _, _, rating, _)
    => rating
    }.reduce{ case (d1,d2) => d1+d2} // can also use sum()

    globalAvgRating = ratingsTotal / ratingsCount
  }

  def predict(userId: Int, movieId: Int): Double = {
    val userAvgRating = userAvgRatings(userId)
    if (userAvgRating == None){ // If user has no rating in the training set simply use the global average
      globalAvgRating
    }
    else{
      val movieGlobalDev = globalAvgDeviationsMap(movieId)
      if (movieGlobalDev == 0 || movieGlobalDev == None) { // if ˆr•,m = 0, or there is no rating for m in the training set, then pu,m =  ̄ru,•
        userAvgRating
      }
      else{
        val prediction = userAvgRating + movieGlobalDev * scale(movieGlobalDev + userAvgRating, userAvgRating)
        prediction
      }
    }
  }
}
