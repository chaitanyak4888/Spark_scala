package Aadhar_card.Set2

import Aadhar_card.Set2.Set2Solution1.aadharSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

object Set2Solution2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("set2").master("local").getOrCreate()
    //imported the aadharSchema from Set2Solution1 code which has aadharSchema method
    val aadharDF  = sparkSession.read.option("header",false).schema(aadharSchema) .csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\aadhaar_data.csv")
    aadharDF.printSchema()
    Soultion2(sparkSession,aadharDF)
  }

  def Soultion2(session: SparkSession, aadharDF: DataFrame) ={
    import org.apache.spark.sql.functions.countDistinct
    // no of distinct states
    aadharDF.select(countDistinct("state")).show()

    //no of different districts in each state
    val distinctDistrictDF = aadharDF.select("state","district").groupBy("district","state").count()
    distinctDistrictDF.repartition(1).write.option("header",true).mode("Overwrite").csv("out/set2/Solution2_Districts")

    // no of different sub districts in each district
    val distinctSubDistrictDF = aadharDF.select("state","district","sub district").groupBy("sub district","district").count()
    distinctSubDistrictDF.repartition(1).write.option("header",true).mode("Overwrite").csv("out/set2/Solution2_SubDistricts")
  }

}
