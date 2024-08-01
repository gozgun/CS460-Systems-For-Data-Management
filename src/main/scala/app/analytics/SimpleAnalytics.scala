package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var moviesGroupedById_RDD: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearID_RDD: RDD[((Int, Int), Iterable[(Int, Int, Option[Double], Double, Int, Int)])] = null

  private def addYearToRDD(rddInput: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, Int, Option[Double], Double, Int, Int)] = {
    rddInput.map(x => {
      val year = new DateTime(x._5 * 1000L).getYear
      // add the year data as the 6th field in the RDD
      (x._1, x._2, x._3, x._4, x._5, year)
    })
  }

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    // convert the timestamp to year and add it as the 6th field
    val ratingsWithYear = addYearToRDD(ratings)

    // Group by year and ID
    val ratingsGroupedByYearID = ratingsWithYear.groupBy(x => (x._6, x._2))

    // movies RDD by ID
    val moviesGroupedById = movie.map(x => (x._1, (x._1, x._2, x._3))).groupByKey()

    val partitioner = new HashPartitioner(10)
    // key-> id, values-> (id, name, [keyword])
    moviesGroupedById_RDD = moviesGroupedById.partitionBy(partitioner).persist()
    // key-> (year, movie id), values -> (user id, movie id, rating, timestamp, year)
    ratingsGroupedByYearID_RDD = ratingsGroupedByYearID.partitionBy(partitioner).persist()
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    val yearRDD = ratingsGroupedByYearID_RDD.map(x => (x._1._1, 1)) // (year, 1)
    val sumRDD = yearRDD.reduceByKey(_ + _)
    sumRDD
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val popularMap = ratingsGroupedByYearID_RDD
      .map{case ((year, movieID), ratingInfo) => (year, (movieID, ratingInfo.size))}
      .reduceByKey{case (x,y) =>
        if (x._2 > y._2 || (x._2 == y._2 && x._1 > y._1)) x else y}
      .map{ case (year, (movieID, count)) => (year, movieID)}
      .map{ case (year, movieID) => (movieID, year)}

    val movieYearTitleRdd = popularMap
      .join(moviesGroupedById_RDD)
      .flatMap(x => x._2._2.map(y => (x._2._1, y._2)))

    movieYearTitleRdd
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val popularMap = ratingsGroupedByYearID_RDD
      .map { case ((year, movieID), ratingInfo) => (year, (movieID, ratingInfo.size)) }
      .reduceByKey { case (x, y) =>
        if (x._2 > y._2 || (x._2 == y._2 && x._1 > y._1)) x else y
      }
      .map { case (year, (movieID, count)) => (year, movieID) }
      .map { case (year, movieID) => (movieID, year) }

    //RDD[(Int, (Int, Iterable[(Int, String, List[String])]))]
    val movieYearTitleRdd = popularMap
      .join(moviesGroupedById_RDD)
      .flatMap(x => x._2._2.map(y => (x._2._1, y._3)))

    movieYearTitleRdd
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val genreCounts = getMostRatedGenreEachYear
      .flatMap { case (_, genres) => genres }
      .countByValue()

    // Sort the genres by count, then lexicographically on genre if there is a tie
    val sortedGenres = genreCounts.toSeq
      .sortBy { case (genre, count) => (count, genre) }
      .map { case (genre, count) => genre }

    // Get the most frequent and least frequent genres
    val mostFrequentGenre = (sortedGenres.last, genreCounts(sortedGenres.last).toInt)
    val leastFrequentGenre = (sortedGenres.head, genreCounts(sortedGenres.head).toInt)

    (leastFrequentGenre, mostFrequentGenre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {

    // compare the required genres and the movie genres as a set
    val requiredGenresSet = requiredGenres.collect().toSet
    val filteredRDD = movies.filter { case (_, _, genres) => requiredGenresSet.subsetOf(genres.toSet) }

    // extract movie titles
    val titlesRDD = filteredRDD.map { case (_, title, _) => title }

    titlesRDD
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {

      val requiredGenres_broadcast = broadcastCallback(requiredGenres)

      val filtered_movies_RDD = movies.filter { case (_, _, genres) => requiredGenres_broadcast.value.toSet.subsetOf(genres.toSet) }
      val movie_titles_RDD = filtered_movies_RDD.map { case (_, title, _) => title }

      movie_titles_RDD
  }

}

