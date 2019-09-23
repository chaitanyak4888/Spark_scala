package Olympic_data


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.macros.whitebox

object Solutions {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Olympic").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val olympicRDD  = sparkContext.textFile("input/olympic_data.csv")
    val splitedRDD  = olympicRDD.map(f=>f.split(","))
   // solution1(sparkContext,splitedRDD)
  //  solution2(sparkContext,splitedRDD)
  def olympicSchema= StructType(
    Array(StructField("Athlete",StringType,true),
      StructField("Age",IntegerType,false),
      StructField("Country",StringType,true),
      StructField("Year",IntegerType,true),
      StructField("Closing",StringType,true),
      StructField("Sports",StringType,true),
      StructField("gold",IntegerType,true),
      StructField("silver",IntegerType,true),
      StructField("bronze",IntegerType,true),
      StructField("total",IntegerType,true)
    ))

    val sparkSession = SparkSession.builder().appName("Olympic").master("local").getOrCreate()
    val olympicDF  = sparkSession.read.schema(olympicSchema).csv("input/olympic_data.csv")
    olympicDF.createOrReplaceTempView("OlympicTable")

    olympicDF.printSchema()
    olympicDF.show(4)
   // solution2DF(sparkSession,olympicDF)
//    solution3DF(sparkSession,olympicDF)
//    solution4DF(sparkSession,olympicDF)
 //   solution5DF(sparkSession,olympicDF)
//    solution6DF(sparkSession,olympicDF)
    solution8DF(sparkSession,olympicDF)
  }
    //No of athletes participated in each Olympic event
    def solution1(sparkContext: SparkContext,splitedRDD : RDD[Array[String]]): Unit ={
      val groupBySport = splitedRDD.map(f=>(f(5),1)).groupBy(f=>f._1).map(f=>(f._1,f._2.reduce((x,y)=>(x._1,x._2+y._2)))).sortByKey()
      groupBySport.saveAsTextFile("out/Olympic_data/solution1.csv")
    }
    //No of medals each country won in each Olympic in ascending order
    def solution2(sparkContext: SparkContext,splitedRDD :RDD[Array[String]]): Unit ={
      val medalsRDD = splitedRDD.map(f=>(f(5),(f(2),f(9)))).groupBy(_._1)
        .map(f=> {
          val sorting = f._2.toList.sortBy(f=>f._2._2)
          val Country = f._2.map(f=>(f._2._1,sorting))
          (f._1,Country)}
           ).foreach(println)
    }
    def solution2DF(sparkSession: SparkSession, olympicDF : DataFrame): Unit ={
//      val results = olympicDF.select("*").groupBy("Country","total","Sports").count().orderBy("Country","Sports")
      //val results1= results.repartition(1).write.mode("Overwrite").option("header",true).csv("out/Olympic_data/Solution2.csv")
      import org.apache.spark.sql.functions.{col,sum}
      olympicDF.groupBy("Year","Country").agg(sum("total").alias("cnt")).orderBy(col("Country").desc).show()

        //                     OR


      sparkSession.sql("SELECT Country,Year,sum(total) as cnt from OlympicTable group by Country,Year order by Country,cnt desc").show()

    }
  //Top 10 athletes who won highest gold medals in all the Olympic events
  def solution3DF(sparkSession: SparkSession, olympicDF : DataFrame)= {
    println("using sql query  ")
      sparkSession.sql("SELECT Athlete , sum(gold) as won from Olympictable group by Athlete order by won desc").show()
    import org.apache.spark.sql.functions.{col,sum}
    println("using functions")
    olympicDF.select("Athlete","gold")
      .groupBy("Athlete")
      .agg(sum("gold").alias("won"))
      .orderBy(col("won").desc).show()
    }
  //No of athletes who won gold and whose age is less than 20
  def solution4DF(sparkSession: SparkSession,olympicDF :DataFrame)={

    import org.apache.spark.sql.functions.{sum}
    olympicDF.select("Athlete","gold")
      .where("Age < 20")
      .where("gold > 0")
      .groupBy("Athlete")
      .agg(sum("gold").alias("won_under_age20"))
      .orderBy("Athlete").show()

  }
  //Youngest athlete who won gold in each category of sports in each Olympic
  def solution5DF(sparkSession: SparkSession,olympicDF :DataFrame)={
        println("Youngest athlete who won gold in each category of sports in each Olympic")

    import org.apache.spark.sql.functions.min
    //val yearSportWindow = Window.partitionBy("Year","Sports").orderBy("Year","Sports")

    //partitioning the year and sports column and then we must and should order by a column( age) to do the partition
    sparkSession.sql("select * from (select Athlete,Age,Sports,gold,Year,dense_rank() over (partition by Year,Sports order by age) as  dr " +
      "   from OlympicTable where gold > 0) where dr = 1 order by Sports" ).repartition(1).write.mode("Overwrite").option("header",true).csv("out/Olympic_data/Solution5df")
  }
  //No of atheletes from each country who has won a medal in each Olympic in each sports
  def solution6DF(sparkSession: SparkSession,olympicDF : DataFrame)={
    olympicDF.where("total > 0").alias("medals_achived").groupBy("Country").count().orderBy("Country").repartition(1).write.option("header",true).csv("out/Olympic_data/Solution6df")
  }
  //Country won highest no of medals in wrestling in 2012
  def solution8DF(sparkSession: SparkSession,olympicDF :DataFrame)={
    import org.apache.spark.sql.functions.col
    olympicDF.selectExpr("*").where("Sports =='Wrestling' AND Year == 2012").groupBy("Country").count().orderBy(col("count").desc).show()
  }
}
