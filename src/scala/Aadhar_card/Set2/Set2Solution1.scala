package Aadhar_card.Set2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Set2Solution1{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("set2").master("local").getOrCreate()
    val aadharDF  = sparkSession.read.option("header",false).schema(aadharSchema) .csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\aadhaar_data.csv")
    aadharDF.printSchema()
    Soultion1(sparkSession,aadharDF)

  }


  def aadharSchema =StructType(
    Array(StructField("Date",IntegerType,true),
      StructField("Registrar",StringType,true),
      StructField("Private Agency",StringType,true),
      StructField("State",StringType,true),
      StructField("District",StringType,true),
      StructField("Sub District",StringType,true),
      StructField("Pin Code",IntegerType,true),
      StructField("Gender",StringType,true),
      StructField("Age",IntegerType,true),
      StructField("Aadhar Generated",IntegerType,true),
      StructField("Rejected",IntegerType,true),
      StructField("Mobile No",IntegerType,true),
      StructField("Email Id",IntegerType,true)
        )
       )

  def Soultion1(sparkSession: SparkSession, aadharDF : DataFrame ): Unit ={
    //using sql.functions countDistinct function to get to count of Distinct registrars
    import org.apache.spark.sql.functions.countDistinct
    aadharDF.select(countDistinct("Registrar")).show()
    //val res=aadharDF.select("registrar").distinct().count()
    //using groupby and count getting each registrar count and writing into csv after repartition into one file
    val resultDF = aadharDF.select("registrar").groupBy("registrar").count()
    resultDF.repartition(1).write.csv("out/solution1.csv")
    println(resultDF.show())


    //sparkSession.sparkContext.parallelize((Seq(res1))).saveAsTextFile("input/solution1.txt")
   // println(res)
  }
}