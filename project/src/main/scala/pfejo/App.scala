package pfejo

import org.apache.spark.{SparkConf, SparkContext}

object App {
  

  def main(args : Array[String]) {
    println( "pfejo starting!" )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark_pfe_jo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5))
    rdd1.collect().foreach(println)
  }

}
