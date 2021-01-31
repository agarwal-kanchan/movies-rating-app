# movies-rating-app

### Laguage and frameworks used</h3>

This application is implemented using scala 2.12.6 and spark 3.0. Build tool used is sbt.


###  Steps to run the application </h3>

1. Checkout application in <***> dir.
2. Go to <***> dir and run following command <b>sbt package</b>
3. Above command will create jar file in <***>/target/scala-2.12/ directory
4. Run the application using following command:
<b>spark-submit --class com.application.MovieRatingApplication "path-of-jar-file" "path-of-data-directory" </b>

e.g. 

<b> spark-submit --class com.application.MovieRatingApplication /Users/xyz/Desktop/Projects/movie-rating-app/target/scala-2.12/movie-rating-app_2.12-0.1.jar /Users/xyz/Desktop/ml-1m/ </b>

Please make sure path of data directory should be full path including / in the end(e.g. /Users/xyz/Desktop/ml-1m/) if you run on MAC or Linux.

Application should work on Window as well with data directory path as per respective format (e.g. C:\Users\xyz\Desktop\ml-1m\ )
Though it has not been tested on Windows due to unavalibility of system.

<h3> Result </h3>

Dataframe will be stored in parquet format in the result_**** subdirectories under the data directory provided at run time.
