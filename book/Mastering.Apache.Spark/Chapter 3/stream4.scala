
// Example of streaming from Apache Flume

package nz.co.semtechsolutions


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume._


object stream4 {

  def main(args: Array[String]) {

    // check the arguments

    if ( args.length < 2 ) 
    {
      System.err.println("Usage: stream4 <host> <port>")
      System.exit(1)
    }

    val hostname = args(0).trim
    val portnum  = args(1).toInt

    println("hostname : " + hostname)
    println("portnum  : " + portnum)

    // create  a spark conf and context

    val appName = "Stream example 4"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // set up the contexts

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10) ) 

    // read from the socket stream

    val rawDstream = FlumeUtils.createStream(ssc,hostname,portnum)

    // print the stream count for debug

    rawDstream.count().map(cnt => ">>>>>>>>>>>>>>> Received events : " + cnt ).print()

    // dump the stream data for debug (netcat) remember to comment out !

    rawDstream.map(e => new String(e.event.getBody.array() ))
              .print

    // start stream and await termination

    ssc.start()
    ssc.awaitTermination()

  } // end main

} // end stream4

