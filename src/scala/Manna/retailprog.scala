package Manna

import java.lang.Float

import org.apache.spark.{SparkConf, SparkContext}
object retailprog {
  def main(args :Array[String])
  {
    val conf=new SparkConf().setAppName("retail")
    val sc=new SparkContext(conf)
    if (args.length < 2)
    {
      println("error")
      System.exit(1)
    }
    val rdd=sc.textFile(args(0))
    val line=rdd.map{line=> {
      val tokens=line.split("\\t")
      (tokens(3),Float.parseFloat(tokens(4)))
    }}
      .reduceByKey(_+_)

    line.saveAsTextFile(args(1))
    sc.stop()
  }
}
