
// Example of streaming frm files in HDFS based directory

package nz.co.semtechsolutions


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object stream3 {

  def main(args: Array[String]) {

    // check the arguments

    if ( args.length < 1 ) 
    {
      System.err.println("Usage: stream3 <directory> ")
      System.exit(1)
    }

    val directory = args(0).trim

    // create  a spark conf and context

    val appName = "Stream example 3"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // set up the contexts

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10) ) 

    // read from the socket stream

    val rawDstream = ssc.textFileStream( directory )

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

} // end stream3

