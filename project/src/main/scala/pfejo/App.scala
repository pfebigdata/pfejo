package com.premierScala.spark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, when,max,sum,desc,rank,asc}
object PremierScala {
  def main(args:Array[String]) : Unit ={
    calcul_nbr_medals_per_athlete
//    calcul_nbr_medals_per_country
//    calcul_average_age_per_year
//    calcul_top5_players_per_country_per_sport
//    calcul_top5_players_per_year_per_country
//    calcul_top5_players_last_year_per_sport
//    calcul_top5_players_last_year_per_country
//    calcul_top5_players_last_year_by_sex
//    calcul_top5_players_last_year("LAST_YEAR")
  }
  def calcul_nbr_medals_per_athlete() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select( "ID","Name","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy("ID","Name").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select *  FROM table_all_data order by score desc """
    spark.sql(query_latest_rec).show()
  }
  def calcul_nbr_medals_per_country() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select( "NOC","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy("NOC").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select *  FROM table_all_data order by score desc """
    spark.sql(query_latest_rec).show()
  }
  def calcul_average_age_per_year() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    co.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select Year,avg(Age) from table_all_data group by Year """
    spark.sql(query_latest_rec).show()
  }
  def calcul_top5_players_per_country_per_sport() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select( "NOC","Sport","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy("NOC","Sport").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select * from (SELECT *, RANK() OVER (PARTITION BY NOC ORDER BY score desc) AS rank FROM table_all_data)  where rank<=5 order by NOC,score desc """
    spark.sql(query_latest_rec).show()
  }
  def calcul_top5_players_per_year_per_country() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select( "Year","NOC","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy("Year","NOC").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select * from (SELECT *, RANK() OVER (PARTITION BY Year ORDER BY score desc) AS rank FROM table_all_data)  where rank<=5 order by Year,score desc """
    spark.sql(query_latest_rec).show()
  }
  def calcul_top5_players_last_year_per_sport() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select("Sport","ID","Name","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy("Sport","ID","Name").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select * from (SELECT *, RANK() OVER (PARTITION BY Sport ORDER BY score desc) AS rank FROM table_all_data)  where rank<=5 order by Sport,score desc """
    spark.sql(query_latest_rec).show()
  }

  def calcul_top5_players_last_year_per_country() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val alldata = co.select( "NOC","ID","Name","Medal").
      where("Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy( "NOC","ID","Name").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """select * from (SELECT *, RANK() OVER (PARTITION BY NOC ORDER BY score desc) AS rank FROM table_all_data)  where rank<=5 order by NOC,score desc """
    spark.sql(query_latest_rec).show()
  }

  def calcul_top5_players_last_year_by_sex() : Unit ={
    calcul_top5_players_last_year("LAST_YEAR_BY_SEX")
  }
  def calcul_top5_players_last_year(typeResultat:String) : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    import org.apache.spark.sql.expressions.Window
    val theLastYear = co.groupBy().agg(max("Year")).first().getString(0)
    val alldata = co.select( "Year","ID","Name", "SEX", "Medal").
      withColumn("lastYear", lit(theLastYear)).
      where("year = lastYear and Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1).otherwise(0)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1).otherwise(0)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1).otherwise(0)).
      groupBy( "Year","ID","Name", "SEX").
      agg( (sum("nbGold")*10000+sum("nbSilver")*100+sum("nbBronze")).alias("score") ,  sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze"))
    alldata.createOrReplaceTempView("table_all_data")
    if(typeResultat=="LAST_YEAR"){
      val query_latest_rec = """SELECT * FROM table_all_data ORDER BY score DESC LIMIT 5 """
      spark.sql(query_latest_rec).show()
    }
    if(typeResultat=="LAST_YEAR_BY_SEX"){
      val query_latest_rec = """SELECT * FROM (SELECT * FROM table_all_data where sex ='M' ORDER BY score DESC LIMIT 5) UNION ALL (SELECT * FROM table_all_data where sex ='F' ORDER BY score DESC LIMIT 5)"""
      spark.sql(query_latest_rec).show()
    }

  }
  def calcul_top5_playersold() : Unit ={
    val spark = SparkSession.builder().appName("PremiereClass").master("local").getOrCreate()
    val co = spark.read.format("csv").
      option("delimiter",",").
      option("escape","\"").
      option("nullValue","NA").
      load("hdfs:///user/hdfsjo/brut.csv").
      toDF("ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal")
    val theLastYear = co.groupBy().agg(max("Year")).first().getString(0)
    val alldata = co.select( "Year","ID","Name", "SEX", "Medal").
      withColumn("lastYear", lit(theLastYear)).
      where("year = lastYear and Medal is not null").
      withColumn("nbGold", when(col("Medal")=== "Gold",1)).
      withColumn("nbSilver", when(col("Medal")=== "Silver",1)).
      withColumn("nbBronze", when(col("Medal")=== "Bronze",1)).
      groupBy( "Year","ID","Name", "SEX").
      agg(sum("nbGold").alias("sumGold"), sum("nbSilver").alias("sumSilver"),sum("nbBronze").alias("sumBronze")).
      withColumn("e", rank().over(Window.partitionBy("SEX").orderBy("sumGold","sumSilver","sumBronze")))
    alldata.createOrReplaceTempView("table_all_data")
    val query_latest_rec = """SELECT * FROM table_all_data ORDER BY e DESC limit 5"""
    spark.sql(query_latest_rec).show()
  }
}
