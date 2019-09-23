package Retail


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._

object Solution1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    SessionDF
  }


  def SessionDF = {
    val sparkSession = SparkSession.builder()
      .appName("Retail")
      .master("local")
      .getOrCreate()

    val retailSchema : StructType = StructType(
      List(StructField("Dates",StringType ,true),
        StructField("Time",StringType,true),
        StructField("City",StringType,true),
        StructField("Product",StringType,true),
        StructField("Sales_volume",FloatType,true),
        types.StructField("Payment",StringType,true)
        ))

    val  retailDF = sparkSession.read.schema(retailSchema).option("inferSchema","true").option("delimiter","\\t").option("dateFormat","yyyy-mm-dd ").csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\scala-spark-tutorial-master\\Asdhar_card\\input\\retail.txt")

//Solution1
    retailDF.groupBy("Product").pivot("City").sum("Sales_volume"). show()

    //Solution2
    retailDF.groupBy("City").pivot("Product").sum("Sales_volume").show()
    retailDF.groupBy("City").agg(sum("Sales_volume")).show()
    retailDF.printSchema()
    retailDF.show(5)
    /*retailDF.select(month(col("Dates"))).show(5)
    val added = retailDF.withColumn("cuurent_Date",current_date()).withColumn("current_time_stamp",current_timestamp())
    val  added1 = retailDF.withColumn("cuurent_Date",to_date(col("Dates")))
    added1.show()
    added1.printSchema()
*/
    //added.repartition(1).write.mode("Overwrite").csv("out/Retail_data")



  }
}


