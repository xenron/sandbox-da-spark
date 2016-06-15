
// checkpoint example 

package nz.co.semtechsolutions

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._


object stream1 {

  // --------------------------------------------------------------------------------

  def createContext( cpDir : String ) : StreamingContext = {
  
    // create  a spark conf and context

    val appName = "Stream example 1"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    val sc = new SparkContext(conf)

    val ssc    = new StreamingContext(sc, Seconds(5) ) 

    ssc.checkpoint( cpDir )

    ssc
  }

  // --------------------------------------------------------------------------------

  def main(args: Array[String]) {

    val hdfsDir = "/data/spark/checkpoint"

    // set twitter auth key values 
    //  see https://apps.twitter.com/
    
    val consumerKey       = "QQpl8EvUW3kPYggwevjhdxNDa"
    val consumerSecret    = "0HFzqt4d8cJhmZfiH3wRgJrqvfadMLfpWVec03DTHbueYfOTwr"
    val accessToken       = "3239476579-17qf1iYE1mgY9QNdfLwR2YyID7LtxnMlLB2Tp1V"
    val accessTokenSecret = "IlQvscoV8NBk1IqPCRwP0dV35htPX9MAUo6fLzYTLLVQI"

    // set twitter auth properties

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // create the stream with a batch interval of x seconds and a window length of y

    val ssc = StreamingContext.getOrCreate(hdfsDir,
      () => { createContext( hdfsDir ) })

    val stream = TwitterUtils.createStream(ssc,None).window( Seconds(60) )

    // do some processing



    // start stream and await termination

    ssc.start()
    ssc.awaitTermination()


  } // end main

} // end stream1

