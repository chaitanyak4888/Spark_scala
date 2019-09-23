package Aadhar_card.Set3

import Aadhar_card.Set2.Set2Solution1.aadharSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

object Set3Solution1 extends App {

    val sparkSession = SparkSession.builder().appName("set2").master("local").getOrCreate()
    //imported the aadharSchema from Set2Solution1 code which has aadharSchema method
    val aadharDF  = sparkSession.read.option("header",false).schema(aadharSchema) .csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\aadhaar_data.csv")
    aadharDF.printSchema()
  //Solution1(sparkSession,aadharDF)
  //Solution3(sparkSession,aadharDF)
 // Solution4(sparkSession,aadharDF)
  Solution5(sparkSession,aadharDF)
import org.apache.spark.sql.functions.{col,expr,desc,sum}
  def Solution1(sparkSession: SparkSession,aadharDF :DataFrame): Unit ={
    aadharDF.select("*").orderBy(col("Aadhar Generated").desc).show(3)
    aadharDF.select("*").orderBy(desc("Aadhar Generated")).show(3)
  }
  //Find the number of residents providing email, mobile number? (Hint: consider non-zero values.)
  def Solution3(sparkSession: SparkSession,aadharDF :DataFrame): Unit ={
    val result = aadharDF.select("*").where(aadharDF.col("Email Id")>0 && aadharDF.col("Mobile No")>0).count()
    println(result)
  }
  //Find top 3 districts where enrolment numbers are maximum?
  def Solution4(sparkSession: SparkSession,aadharDF :DataFrame)={
   // aadharDF.selectExpr("'Aadhar Generated' + Rejected").alias("enrolment").show() //.orderBy(col("enrolment").desc).show(3)
   /* aadharDF.createOrReplaceTempView("aadhar")
    sparkSession.sql("select Aadhar Generated + Rejected from aadhar").show()*/
    aadharDF.select(expr("'Aadhar Generated' + Rejected").alias("enrolment")).show()
    aadharDF.createOrReplaceTempView("aadharTable")
//    sparkSession.sql("Select (`Aadhar Generated`+`Rejected`) as Enrollment from aadharTable").show()
    aadharDF.selectExpr("`Aadhar Generated` + Rejected as Enrollment").show()
//    aadharDF.withColumn("enrolment",sum(expr("'Aadhar Generated'  Rejected"))).show()

    //aadharDF.agg( expr("'Aadhar Generated'"),expr("Rejected")).alias("enrolment").orderBy(col("enrolment")).show(3)
  }
  //Find the no. of Aadhaar cards generated in each state?
  def Solution5(sparkSession: SparkSession,aadharDF : DataFrame)={
    aadharDF.selectExpr("State","`Aadhar Generated`+Rejected as Enrollment").groupBy("State").count().show(30)
  }












}
