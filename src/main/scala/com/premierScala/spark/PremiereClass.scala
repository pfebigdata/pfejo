package com.premierScala.spark

import org.apache.spark._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

//import org.joda.time.DateTime
// import org.apache.spark



class PremiereClass {

  def main(args:Array[String]) : Unit ={

    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()

    /*val spark = SparkSession.builder
      .master("local")
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
*/


    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")


    co.select("NOC","ID","Name","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1)).
      groupBy("NOC","ID","Name").
      agg(sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze")).
      withColumn("e", rank().over(   Window.partitionBy("NOC").orderBy($"sumGold".desc , $"sumSilver".desc , $"sumBronze".desc)    )).
      orderBy($"NOC", $"sumGold".desc , $"sumSilver".desc , $"sumBronze".desc).
      where (" e <=5 ").
      show()


  }


}
