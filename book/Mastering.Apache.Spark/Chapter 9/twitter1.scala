
package nz.co.semtechsolutions

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._ 
import org.apache.spark.sql.types.{StructType,StructField,StringType}


object twitter1 {

  def main(args: Array[String]) {

    // create  a spark conf and context

    val appName = "Twitter example 1"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc = new SparkContext(conf)

    // set twitter auth key values 
    //  see https://apps.twitter.com/
    
    val consumerKey       = "QQplxxx"
    val consumerSecret    = "0HFzqxxx"
    val accessToken       = "323947xxx"
    val accessTokenSecret = "IlQvscxxx"

    // set twitter auth properties

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // create the stream with a batch interval of 5 seconds and a window length of 60

    val ssc    = new StreamingContext(sc, Seconds(5) ) 
    val stream = TwitterUtils.createStream(ssc,None).window( Seconds(60) )

    // split out the hash tags from the stream

    val hashTags = stream.flatMap( status => status.getText.split(" ").filter(_.startsWith("#")))

    hashTags.foreachRDD{ rdd =>    

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val dfHashTags = rdd.map(hashT => hashRow(hashT) ).toDF()

      // create a temp table

      dfHashTags.registerTempTable("tweets")

      // lets use this table

      val tweetcount = sqlContext.sql("select count(*) from tweets")

      println("\n================================================================================")
      println(  "================================================================================\n")

      println("Count of hash tags in stream table : " + tweetcount.toString )

      tweetcount.map(c => "Count of hash tags in stream table : " + c(0).toString ).collect().foreach(println)

      println("\n================================================================================")
      println(  "================================================================================\n")

    } // for each hash tags rdd

    // Lets take the top five keywords and print them 

    // see example at
    //  https://github.com/apache/spark/blob/master/examples/src/main/scala/
    //         org/apache/spark/examples/streaming/TwitterPopularTags.scala
     
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {

      val topList = rdd.take(5)

      println("\n================================================================================")
      println(  "================================================================================\n")

      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

      println("\n================================================================================")
      println(  "================================================================================\n")
    })

    // start streama nd await termination

    ssc.start()
    ssc.awaitTermination()

  } // end main

} // end twitter1

//----------------------------------------------------------------------

object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

//----------------------------------------------------------------------

case class hashRow( hashTag: String)

