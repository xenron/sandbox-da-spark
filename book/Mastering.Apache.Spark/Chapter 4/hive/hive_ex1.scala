
// hive on spark example 1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext


object hive_ex1 {

  def main(args: Array[String]) {

    // create  a spark conf and context

    val appName = "Hive Spark Ex 1"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // create contexts

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    import hiveContext.sql

    // run some sql

    // -----------------------------------------------------------
    // example 1

//    hiveContext.sql( """                                
//                                                     
//        CREATE TABLE IF NOT EXISTS adult2
//           (                                          
//             idx             INT,                     
//             age             INT,                     
//             workclass       STRING,                    
//             fnlwgt          INT,                     
//             education       STRING,                    
//             educationnum    INT,                     
//             maritalstatus   STRING,                    
//             occupation      STRING,                    
//             relationship    STRING,                    
//             race            STRING,                    
//             gender          STRING,                    
//             capitalgain     INT,                     
//             capitalloss     INT,                     
//             nativecountry   STRING,                    
//             income          STRING                    
//           )                    
//                    
//                    """)

//    val resRDD = hiveContext.sql("SELECT COUNT(*) FROM adult2")

//    resRDD.map(t => "Count : " + t(0) ).collect().foreach(println)


    // -----------------------------------------------------------
    // example 2

    hiveContext.sql("""

        CREATE EXTERNAL TABLE IF NOT EXISTS adult3
           (                                          
             idx             INT,                     
             age             INT,                     
             workclass       STRING,                    
             fnlwgt          INT,                     
             education       STRING,                    
             educationnum    INT,                     
             maritalstatus   STRING,                    
             occupation      STRING,                    
             relationship    STRING,                    
             race            STRING,                    
             gender          STRING,                    
             capitalgain     INT,                     
             capitalloss     INT,                     
             nativecountry   STRING,                    
             income          STRING                    
           )                    
           ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
           LOCATION '/data/spark/hive'

                   """)

    // take another count 

    // val resRDD = hiveContext.sql("SELECT COUNT(*) FROM adult3")

    // resRDD.map(t => "Count : " + t(0) ).collect().foreach(println)

    // -----------------------------------------------------------
    // example 3

    // val resRDD = hiveContext.sql("""

    //      SELECT t1.edu FROM
    //      ( SELECT DISTINCT education AS edu FROM adult3 ) t1
    //      ORDER BY t1.edu 

    //                """)

    // resRDD.map(t => t(0) ).collect().foreach(println)

    // -----------------------------------------------------------
    // example 4


    // drop and recreate education table

//    hiveContext.sql("""
//
//       DROP TABLE IF EXISTS education
//
//                   """)


//    hiveContext.sql("""
//
//      CREATE TABLE IF NOT EXISTS  education 
//        (
//          idx        INT,                     
//          name       STRING                    
//        )
//        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
//        LOCATION '/data/spark/dim1/'
//
//                   """)
//
//
//    // output result
//
//    val resRDD = hiveContext.sql("""
//
//       SELECT * FROM education
//
//                   """)
//
//    resRDD.map( t => t(0)+" "+t(1) ).collect().foreach(println)




  } // end main

} // end sql1

