package dg.spark.mllib.homework

import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

object ch01 {
  def main(args: Array[String]): Unit = {
    val start = new Date();
    val conf = new SparkConf().setAppName("mllib lesson1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // load all files from file path
    val file = sc.textFile("file:///tmp/test", 1)
    println("top 10 test data")
    file.take(10).foreach(println)

    //1）通过Spark读取安装列表数据，并且统计数据总行数、用户数量、日期有哪几天；
    val count = file.count
    println(s"record count : $count")

    val data = file.map { x =>
      val fields = x.split("\t")
      (fields(0), fields(1), fields(2))
    }.repartition(1)

    val allUserNum = data.map(x => x._2).distinct().count
    println(s"user count : $allUserNum")

    val dateList = data.map(x => x._1).distinct().sortBy(x => x, true, 1)
    println("date list")
    dateList.foreach(println)

    //2）指定任意连续的2天，计算每个用户第2天的新安装包名；
    val day1 = "2016-04-07";
    val day2 = "2016-04-08";
    val installDay1 = data.filter(_._1 == day1).map(x => (x._2, x._3)).distinct();
    val installDay2 = data.filter(_._1 == day2).map(x => (x._2, x._3)).distinct();
    val newInstall = installDay2.subtract(installDay1).sortByKey(true, 1);

    println(s"用户$day1 日比$day2 日新装列表：")
    newInstall.take(100).foreach(println)

    //3）指定任意1天的数据，计算top1000包的支持度和置信度
    val support = 0.1f;
    val confidence = 0.3f;
    val top1000 = installDay1.map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._2, false, 1).take(1000).map(_._1);

    //过滤掉不在top1000中的安装包的记录，按user分组
    val installByUser = installDay1.filter(x => top1000.contains(x._2.asInstanceOf[String])).groupByKey();

    val userNum = installDay1.keys.distinct().count();
    println(s"$day1 日用户数：$userNum");

    var j = 1
    //频繁项集，支持度大于0.1

    val frequentItemset = installByUser.flatMap { x =>
      //println(j + "/" + userNum);
      j += 1;
      val items = x._2.toArray;
      val rtn = items.flatMap { i =>
        items.map { ii =>
          val tuple = if (i < ii) (i, ii) else (ii, i)
          if (i == ii) (tuple, 0) else (tuple, 1)
        }
      };
      rtn.distinct;
    }.reduceByKey(_ + _).map(x => (x._1, x._2, x._2 / userNum.toFloat)).filter(_._3 > support).sortBy(_._2, false, 1)
    println("满足最小支持度$support的记录数：" + frequentItemset.count())
    frequentItemset.foreach(x => printf("%-60s %10d %8f\n", x._1, x._2, x._3));

    println("置信度：")
    println("A => B关联规则  A/B同现次数   支持度A出现次数   置信度")
    println("--------------------------------------------------------------- ----------- -------- ---------- --------")
    //每个item的安装次数，用于计算置信度
    val countByItem = installDay1.map(x => (x._2, 1)).reduceByKey(_ + _).collect().toMap[String, Int];
    //满足支持度>0.1，置信度>0.3的关联规则
    val associationRules = frequentItemset.flatMap { x =>
      val count1 = countByItem.get(x._1._1).getOrElse(0);
      val count2 = countByItem.get(x._1._2).getOrElse(0);
      val confidence1 = if (count1 == 0) 0 else x._2 / count1.toFloat;
      val confidence2 = if (count2 == 0) 0 else x._2 / count2.toFloat;
      Array(((x._1._1, x._1._2), x._2, x._3, count1, confidence1), ((x._1._2, x._1._1), x._2, x._3, count2, confidence2))
    }.filter(_._5 > confidence).sortBy(x => x._5, false, 1);

    associationRules.foreach(x => printf("%-30s => %-30s:%10d %8f %10d %8f\n", x._1._1, x._1._2, x._2, x._3, x._4, x._5));
    val end = new Date();
    println("总耗时：" + (end.getTime() - start.getTime()) / 1000f + "s")
  }
}
