
// Data frame to table and then sql example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._



object sql5 {

  // -------------------------------------------------------------------

  def ageBracket( age:Int ) : Int =
  {
    var bracket = 9999

         if ( age >= 0  && age < 20  ) { bracket = 0 }
    else if ( age >= 20 && age < 40  ) { bracket = 1 }
    else if ( age >= 40 && age < 60  ) { bracket = 2 }
    else if ( age >= 60 && age < 80  ) { bracket = 3 }
    else if ( age >= 80 && age < 100 ) { bracket = 4 }
    else if ( age > 100 )               { bracket = 5 }

    return bracket
  }

  // -------------------------------------------------------------------

  def enumEdu( education:String ) : Int =
  {
    var enumval = 9999

         if ( education.trim == "10th" )         { enumval = 0 }
    else if ( education.trim == "11th" )         { enumval = 1 }
    else if ( education.trim == "12th" )         { enumval = 2 }
    else if ( education.trim == "1st-4th" )      { enumval = 3 }
    else if ( education.trim == "5th-6th" )      { enumval = 4 }
    else if ( education.trim == "7th-8th" )      { enumval = 5 }
    else if ( education.trim == "9th" )          { enumval = 6 }
    else if ( education.trim == "Assoc-acdm" )   { enumval = 7 }
    else if ( education.trim == "Assoc-voc" )    { enumval = 8 }
    else if ( education.trim == "Bachelors" )    { enumval = 9 }
    else if ( education.trim == "Doctorate" )    { enumval = 10 }
    else if ( education.trim == "HS-grad" )      { enumval = 11 }
    else if ( education.trim == "Masters" )      { enumval = 12 }
    else if ( education.trim == "Preschool" )    { enumval = 13 }
    else if ( education.trim == "Prof-school" )  { enumval = 14 }
    else if ( education.trim == "Some-college" ) { enumval = 15 }

    return enumval
  }

  // -------------------------------------------------------------------

  def main(args: Array[String]) {

    // create  a spark conf and context

    val appName = "sql example 5"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // create contexts

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // load some data from hdfs 

    val rawRdd = sc.textFile("hdfs:///data/spark/sql/adult.train.data2")

    // specify a schema

    val schema =
      StructType(
        StructField("idx",                IntegerType, false) :: 
        StructField("age",                IntegerType, false) :: 
        StructField("workclass",          StringType,  false) ::
        StructField("fnlwgt",             IntegerType, false) ::
        StructField("education",          StringType,  false) ::
        StructField("educational-num",    IntegerType, false) ::
        StructField("marital-status",     StringType,  false) ::
        StructField("occupation",         StringType,  false) ::
        StructField("relationship",       StringType,  false) ::
        StructField("race",               StringType,  false) ::
        StructField("gender",             StringType,  false) ::
        StructField("capital-gain",       IntegerType, false) ::
        StructField("capital-loss",       IntegerType, false) ::
        StructField("hours-per-week",     IntegerType, false) ::
        StructField("native-country",     StringType,  false) ::
        StructField("income",             StringType,  false) ::
        Nil)

    // create row data

    val rowRDD = rawRdd.map(_.split(","))
      .map(p => Row( p(0).trim.toInt,
                     p(1).trim.toInt,
                     p(2),
                     p(3).trim.toInt,
                     p(4),
                     p(5).trim.toInt,
                     p(6),
                     p(7),
                     p(8),
                     p(9),
                     p(10),
                     p(11).trim.toInt,
                     p(12).trim.toInt,
                     p(13).trim.toInt,
                     p(14),
                     p(15) 
                   ))

    // create a data frame from schema and row data

    val adultDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // now create a temporary table and run some sql examples

    adultDataFrame.registerTempTable("adult")

    // ---------------------------------------------------
    // example 7

    // val selectClause = "SELECT t1.idx,age,education,occupation,workclass,race,gender FROM "
    // val tableClause1 = " ( SELECT idx,age,education,occupation FROM adult) t1 JOIN " 
    // val tableClause2 = " ( SELECT idx,workclass,race,gender FROM adult) t2 " 
    // val joinClause = " ON (t1.idx=t2.idx) "
    // val limitClause = " LIMIT 10"

    // val resRDD = sqlContext.sql( selectClause + 
    //                              tableClause1 + tableClause2 + 
    //                              joinClause   + limitClause 
    //                            )

    // resRDD.map(t => t(0) + " " + t(1) + " " + t(2) + " " +
    //                 t(3) + " " + t(4) + " " + t(5) + " " + t(6) 
    //           )
    //           .collect().foreach(println)

    // ---------------------------------------------------
    // example 8

    // val selectClause = "SELECT t1.edu_dist FROM "
    // val tableClause  = " ( SELECT DISTINCT education AS edu_dist FROM adult ) t1 " 
    // val orderClause  = " ORDER BY t1.edu_dist " 

    // val resRDD = sqlContext.sql( selectClause + tableClause  + orderClause )

    // resRDD.map(t => t(0)).collect().foreach(println)

    // ---------------------------------------------------
    // example 9

    // sqlContext.udf.register( "enumEdu", enumEdu _ ) 

    // val selectClause = "SELECT enumEdu(t1.edu_dist) as idx,t1.edu_dist FROM "
    // val tableClause  = " ( SELECT DISTINCT education AS edu_dist FROM adult ) t1 " 
    // val orderClause  = " ORDER BY t1.edu_dist " 

    // val resRDD = sqlContext.sql( selectClause + tableClause  + orderClause )

    // resRDD.map(t => t(0) + " " + t(1) ).collect().foreach(println)

    // ---------------------------------------------------
    // example 10

    // sqlContext.udf.register( "ageBracket", ageBracket _ ) 

    // val selectClause = "SELECT age,ageBracket(age) as bracket,education FROM "
    // val tableClause  = " adult " 
    // val limitClause  = " LIMIT 10 " 

    // val resRDD = sqlContext.sql( selectClause + tableClause  + limitClause )

    // resRDD.map(t => t(0) + " " + t(1) + " " + t(2) ).collect().foreach(println)

    // ---------------------------------------------------
    // example 11

    sqlContext.udf.register( "dblAge", (a:Int) => 2*a ) 

    val selectClause = "SELECT age,dblAge(age) FROM "
    val tableClause  = " adult " 
    val limitClause  = " LIMIT 10 " 

    val resRDD = sqlContext.sql( selectClause + tableClause  + limitClause )

    resRDD.map(t => t(0) + " " + t(1) ).collect().foreach(println)



  } // end main

} // end sql5

