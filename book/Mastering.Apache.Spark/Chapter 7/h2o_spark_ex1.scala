
// ---------------------------------------------------------------------------
// This example is for use with spark 1.2 it implements the example 
// at https://github.com/h2oai/sparkling-water/tree/master/examples/scripts
// data available at ../../examples/smalldata
// ---------------------------------------------------------------------------

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import hex.deeplearning.{DeepLearningModel, DeepLearning}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o._
import org.apache.spark.mllib
import org.apache.spark.mllib.feature.{IDFModel, IDF, HashingTF}
import org.apache.spark.rdd.RDD

// import water.Key


object h2o_spark_ex1  extends App
{
  // ---------------------------------------------------------------------------
  // Representation of a training message

  case class SMS(target: String, fv: mllib.linalg.Vector)
  // ---------------------------------------------------------------------------
  // the data loader

  def load(sc:SparkContext,dataFile: String): RDD[Array[String]] = {
    sc.textFile(dataFile).map(l => l.split("\t")).filter(r => !r(0).isEmpty)
  }
  // ---------------------------------------------------------------------------
  // Tokenizer
  def tokenize(data: RDD[String]): RDD[Seq[String]] = {
    val ignoredWords = Seq("the", "a", "", "in", "on", "at", "as", "not", "for")
    val ignoredChars = Seq(',', ':', ';', '/', '<', '>', '"', '.', '(', ')', '?', '-', '\'','!','0', '1')

    val texts = data.map( r=> {
      var smsText = r.toLowerCase
      for( c <- ignoredChars) {
        smsText = smsText.replace(c, ' ')
      }

      val words =smsText.split(" ").filter(w => !ignoredWords.contains(w) && w.length>2).distinct

      words.toSeq
    })
    texts
  }
  // ---------------------------------------------------------------------------
  // Tf-IDF model builder

  def buildIDFModel(tokens: RDD[Seq[String]],
                    minDocFreq:Int = 4,
                    hashSpaceSize:Int = 1 << 10): (HashingTF, IDFModel, RDD[org.apache.spark.mllib.linalg.Vector]) = {
    // Hash strings into the given space
    val hashingTF = new HashingTF(hashSpaceSize)
    val tf = hashingTF.transform(tokens)
    // Build term frequency-inverse document frequency
    val idfModel = new IDF(minDocFreq = minDocFreq).fit(tf)
    val expandedText = idfModel.transform(tf)
    (hashingTF, idfModel, expandedText)
  }
  // ---------------------------------------------------------------------------
  // DeepLearning model builder

  def buildDLModel(train: Frame, 
                   valid: Frame,
                   epochs: Int = 10, 
                   l1: Double = 0.001, 
                   l2: Double = 0.0,
                   hidden: Array[Int] = Array[Int](200, 200))
                (implicit h2oContext: H2OContext): DeepLearningModel = {
    import h2oContext._
    // Build a model
    val dlParams = new DeepLearningParameters()
//    dlParams._destination_key = Key.make("dlModel.hex").asInstanceOf[water.Key[Frame]]
    dlParams._train = train
    dlParams._valid = valid
    dlParams._response_column = 'target
    dlParams._epochs = epochs
    dlParams._l1 = l1
    dlParams._hidden = hidden
  
    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get
  
    // Compute metrics on both datasets
    dlModel.score(train).delete()
    dlModel.score(valid).delete()
  
    dlModel
  }
  // ---------------------------------------------------------------------------
  def isSpam(sc:SparkContext,
             msg: String,
             dlModel: DeepLearningModel,
             hashingTF: HashingTF,
             idfModel: IDFModel,
             hamThreshold: Double = 0.5):Boolean = {
    val msgRdd = sc.parallelize(Seq(msg))

    import h2oContext._
    import org.apache.spark.sql.SchemaRDD 
    import sqlContext._

    val msgVector: SchemaRDD = idfModel.transform(
                                 hashingTF.transform (
                                   tokenize (msgRdd))).map(v => SMS("?", v))
    val msgTable: DataFrame = msgVector
    msgTable.remove(0) // remove first column
    val prediction = dlModel.score(msgTable)
    prediction.vecs()(1).at(0) < hamThreshold
  }
  // ---------------------------------------------------------------------------

  // create  a spark conf and context

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "Spark h2o ex1"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  val sparkCxt = new SparkContext(conf)

  // create the h2o context 

  import org.apache.spark.h2o._
  implicit val h2oContext = new org.apache.spark.h2o.H2OContext(sparkCxt).start()

  // Initialize SQL context

  import h2oContext._
  import org.apache.spark.sql._

  implicit val sqlContext = new SQLContext(sparkCxt)

  // Open H2O UI 

  import sqlContext._
  openFlow

  // Now load and process data 

  val server = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val path   = "/data/spark/h2o/"
  val DATAFILE = server + path + "smsData.txt"

  // Data load

  val data = load(sparkCxt,DATAFILE)

  // Extract response spam or ham
  val hamSpam = data.map( r => r(0))
  val message = data.map( r => r(1))

  // Tokenize message content
  val tokens = tokenize(message)

  // Build Tf-IDF model
  var (hashingTF, idfModel, tfidf) = buildIDFModel(tokens)

  // Merge response with extracted vectors
  val resultRDD: SchemaRDD = hamSpam.zip(tfidf).map(v => SMS(v._1, v._2))

  val table:DataFrame = resultRDD

  // Split table
  val keys = Array[String]("train.hex", "valid.hex")
  val ratios = Array[Double](0.8)
  val frs = split(table, keys, ratios)
  val (train, valid) = (frs(0), frs(1))
  table.delete()

  // Build a model
  val dlModel = buildDLModel(train, valid)

  // Collect model metrics and evaluate model quality
  val trainMetrics = binomialMM(dlModel, train)
  val validMetrics = binomialMM(dlModel, valid)

//  println(trainMetrics.auc.AUC)
//  println(validMetrics.auc.AUC)

  // now try some spam detection

//  isSpam(sparkCxt,"Michal, beer tonight in MV?", 
   //      dlModel, hashingTF, idfModel)

 // isSpam(sparkCxt,"We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", 
  //       dlModel, hashingTF, idfModel)

  // shutdown h20

//  water.H2O.shutdown()

  sparkCxt.stop()

  println( " >>>>> Script Finished <<<<< " )

} // end application

