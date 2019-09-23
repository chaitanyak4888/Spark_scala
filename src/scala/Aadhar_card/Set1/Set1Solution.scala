package Aadhar_card.Set1

import Aadhar_card.Set2.Set2Solution1.aadharSchema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Set1Solution {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession.builder().appName("set2").master("local").getOrCreate()
    val aadharDF  = sparkSession.read.option("header",false).schema(aadharSchema) .csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\aadhaar_data.csv")
    aadharDF.printSchema()
    aadharDF.createOrReplaceTempView("aadhartable")
    val results = sparkSession.sql("select * from (select *,row_number() over (partition by Registrar order by State) store_rank from aadhartable) where store_rank <=25 ")
    results.repartition(1).write.mode("Overwrite").option("header",true).csv("out/Set1Solution")


  }
}
