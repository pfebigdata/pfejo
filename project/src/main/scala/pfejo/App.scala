package pfejo

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello pfejo!" )
    println("concat arguments = " + foo(args))

    val sc = new SparkContext("local", "Word Count (2)", new SparkConf())

    try {

      val out = "output/kjv-wc2"
      FileUtil.rmrf(out)
      val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
      val wc = input
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .map(word => (word, 1))
        .reduceByKey((count1, count2) => count1 + count2)
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()      // Stop (shut down) the context.
    }
  }

}
