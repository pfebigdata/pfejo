package pfejo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, when,sum,desc,rank}
object App2 {
  def main(args : Array[String]) {
    println( "pfejo starting!" )
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
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
      withColumn("e", rank().over(   Window.partitionBy("NOC").orderBy(col("sumGold").desc , col("sumSilver").desc , col("sumBronze").desc))).
      //orderBy("NOC", col("sumGold").desc , col("sumSilver").desc , col("sumBronze").desc).
      where (" e <=5 ").
      show()
  }
}
