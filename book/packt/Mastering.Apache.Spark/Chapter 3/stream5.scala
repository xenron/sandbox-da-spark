
// Example of streaming from Apache Flume

package nz.co.semtechsolutions


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume._
import org.json4s._
import org.json4s.jackson.Serialization.{read,write}


object stream5 {

  def main(args: Array[String]) {

    // check the arguments

    if ( args.length < 2 ) 
    {
      System.err.println("Usage: stream5 <host> <port>")
      System.exit(1)
    }

    val hostname = args(0).trim
    val portnum  = args(1).toInt

    println("hostname : " + hostname)
    println("portnum  : " + portnum)

    // create  a spark conf and context

    val appName = "Stream example 5"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // set up the contexts

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10) ) 

    // read from the socket stream

    val rawDstream = FlumeUtils.createStream(ssc,hostname,portnum)

    // print the stream count for debug

    rawDstream.count().map(cnt => ">>>>>>>>>>>>>>> Received events : " + cnt ).print()

    // process the rss data 

    case class RSSItem(category : String, title : String, summary : String)

    val now: Long = System.currentTimeMillis

    val hdfsdir = "hdfs://hc2nn:8020/data/spark/flume/rss/"

    // format the events to a string and write them to hdfs

    rawDstream.map(record => {
	implicit val formats = DefaultFormats
        read[RSSItem](new String(record.event.getBody().array()))
    })
         .foreachRDD(rdd => {
	    if (rdd.count() > 0) {
              rdd.map(item => {
                implicit val formats = DefaultFormats
                write(item)
		  }).saveAsTextFile(hdfsdir+"file_"+now.toString())
            }
    })

    // start stream and await termination

    ssc.start()
    ssc.awaitTermination()

  } // end main

} // end stream5

