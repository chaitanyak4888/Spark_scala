package Aadhar_card.Set2

import Aadhar_card.Set2.Set2Solution1.aadharSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

object Set2Solution3 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("set2").master("local").getOrCreate()
    //imported the aadharSchema from Set2Solution1 code which has aadharSchema method
    val aadharDF  = sparkSession.read.option("header",false).schema(aadharSchema) .csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\aadhaar_data.csv")
    aadharDF.printSchema()
  //  Solution3(sparkSession,aadharDF)
    Solution4(sparkSession,aadharDF)
  }

  def Solution3(sparkSession: SparkSession,aadharDF :DataFrame) ={

//    aadharDF.createTempView("aadhar")
    aadharDF.createOrReplaceTempView("aadhar")
    sparkSession.sql("SELECT * FROM aadhar").show(5)
    sparkSession.sql("SELECT Gender,State ,count(1) from aadhar GROUP BY State,Gender ORDER BY State")

    aadharDF.select("*").groupBy("State","Gender").count().show(30)
  }

  def Solution4(sparkSession: SparkSession,aadharDF :DataFrame)={
  aadharDF.select("*").groupBy("State", "Private Agency").count().show()
  }

}
