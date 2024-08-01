# CS 460 Project Details

## Task 1
To load the data, MoviesLoader and RatingsLoader files were modified. 

MoviesLoader: To save the keywords of the movies as a list, the quotation mark has been removed. After the cleanup, the data is distributed among fields according to the project guidelends and the RDD has been persisted.

RatingsLoader: The data is distributed among fields according to the project guidelends and the RDD has been persisted.

## Task 2
### Task 2.1
The task of grouping movies by their ID has been done by using groupByKey() method.

To create an RDD where keys are year and movie ID are done by using groupBy. To extract the year information a new method called addYearToRDD() has been written. It takes the old RDD and returns a new RDD with a 6th field that contains the year. Note that it could also replace the 5th field.

In addYearToRDD(), DateTime object from org.joda.time has been used. Since DateTime object takes miliseconds as an input, the timestamp has been converted to the right order. Then the getYear() method of DateTime has been used to extract the year.

### Task 2.2
To get the number of movies rated each year, (year, 1) tuple has been created and the second field of each tuple has been added by using reduceByKey() method.

### Task 2.3
To get the most popular movie for each year, the year information is used as key, and then the number of ratings is counted. In the case of equality, movie id value is compared.

### Task 2.4
Same algorithm from Task 2.3 is used, here we also projected the movie genres by joining the movies grouped by Id RDD.

### Task 2.5
By using flatMap and countByValue(), we counted the occurance of each genre among the genre's of popular movies. Then they are sorted by their counts in ascending order then by lexicographically on genre. To get the most frequent one, we got the tail element and for the least frequent one, we got the head element.

### Task 2.6
Movies are filtered by the specified list of genre, then the title information is extracted.

### Task 2.7
Similar methods in Task 2.6 are used. Additionally, the broadcastCallback function is called to create a broadcast variable of that requiredGenres parameter.

## Task 3
### Task 3.1
To merge information on movies and ratings, we use rightOuterJoin on movie id. If there are no ratings available for the movie, it is set to 0, else the rating is assigned. Count of ratings, sum of ratings and the average of ratings (sum of ratings / count of ratings) fields are added to the final RDD as instructed.

Movie titles and the average ratings are projected in getResult()

### Task 3.2
First, we filter the movies that contain the specified keywords. If there are none, -1 is returned. Else, the number of votes and the sum of averages are calculated and their average is returned.

### Task 3.3
There are several cases to consider when there is an update to a rating. 
If there is an old rating, we need to remove it and add the new one to the sum and update the average. The count does not change.
If there were no ratings before, then we add it to the sum, increment the count and update the average.
To save this new RDD, we remove the old one and persist the new one.

## Task 4
### Task 4.1
To compute the signature of each title, minhash.hash() function is used. 
To cluster titles with same signatures, groupByKey on signatures is used.
Partitioning is not implementd.

### Task 4.2
The list of buckets are retrieved from the LSH index using the getBuckets() method. Then this list and the input queries are joined on the bucket ID as the key. This will pair each query with the list of buckets containing similar queries.

### Task 4.3
Inside init(), some Maps are defined by convenience and efficiency. Maps are more suitable than RDDs for search, thus Map is more preferred in this task.

1. userNormalizedDeviationsMap: The key is user id and it contains (movie id, global average deviation) pairs as a value. To extract this information, the ratingsRDD has been mapped to (user id, (movie id, rating). Then user's average rating was calculated, to compute the deviation user's rating for the movie and user's average rating has been given as parameter to the scale() function. The rules in scale function has been defined by the documentation.
2. globalAvgDeviationsMap: It contains movie id and its global average deviation. To calculate the global average deviation, userNormalizedDeviationsRDD was used. The RDD was grouped by movie id, and if there are no entry for the movie, 0 was returned, else the average of deviations for all users was returned.
3. userAvgRatings: Contains the users' average ratings.
4. globalAvgRating (Double): Contains the global average rating.

Predict function is written according to the documentation rules, also the cases such as user or movie having no ratings are handled here.

NOTE: import scala.collection.Map has been added to be able to initialize and update Map variables. Before this, the Map objects are created immutable by default. (A TA has been consulted about this.)

### Task 4.4
To train the model, ALS.train() function is used. The parameters of this function is defined by class' parameters and variables.
To predict the rating given the movie id and user id, model.predict() is called.

### Task 4.5
recommendCollaborative and recommendBaseline functions share almost the same code, thus the explanation holds for both.

First, the id of movies who are similar to the specified genre was found by using nn_lookup.
Then, ratings of the specified user is extracted.
To recommend movies, the appropriate predictor functions were called. Then the predicted ratings for these movies are sorted in descending order and then the first K movies are returned.
To avoid recommending movies a user has already rated, the movie id's presence was checked among specified user's ratings.
