
// Example of streaming via tcp using netcat host and port

package nz.co.semtechsolutions


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object stream2 {

  def main(args: Array[String]) {

    // check the arguments

    if ( args.length < 2 ) 
    {
      System.err.println("Usage: stream2 <host> <port>")
      System.exit(1)
    }

    val hostname = args(0).trim
    val portnum  = args(1).toInt

    // create  a spark conf and context

    val appName = "Stream example 2"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // set up the contexts

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10) ) 

    // read from the socket stream

    val rawDstream = ssc.socketTextStream( hostname, portnum )

    // get the top 10 words

    val wordCount = rawDstream
                     .flatMap(line => line.split(" "))
                     .map(word => (word,1))
                     .reduceByKey(_+_)                 
                     .map(item => item.swap)
                     .transform(rdd => rdd.sortByKey(false))
                     .foreachRDD( rdd => 
                       { rdd.take(10).foreach(x=>println("List : " + x)) })

    // start stream and await termination

    ssc.start()
    ssc.awaitTermination()

  } // end main

} // end stream2

