package Manna

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Solution2 {
  def main(args: Array[String]): Unit = {
  val sparkSession = SparkSession.builder().master("local").appName("Solution2").getOrCreate()
    val peopleDf = sparkSession.read.option("inferSchema",true).csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\Use cases\\people.csv")
    val peopleidPartition = Window.partitionBy("people","id").orderBy("id")



  }
}
