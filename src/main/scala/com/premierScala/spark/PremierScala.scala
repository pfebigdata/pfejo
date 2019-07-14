package com.premierScala.spark

import org.apache.spark.{SparkConf, SparkContext}

object PremierScala {

  def main(args:Array[String]) : Unit ={

    val conf = new SparkConf()

    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(1,2,3,4,5))

    rdd1.collect().foreach(println)

  }
}
