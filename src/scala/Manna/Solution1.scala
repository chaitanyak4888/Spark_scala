package Manna

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import java.sql.Date

import org.apache.spark.sql.types.DateType


object Solution1 {

  case class userSchema(userID: Int, banned: String, Role: String)

  case class tripSchema(ID: Int, Client_id: Int, Driver_id: Int, City_id: Int, Status: String, Request_time: Date)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val userSchema1 = Encoders.product[userSchema].schema
    val tripSchema1 = Encoders.product[tripSchema].schema

    val sparkSession = SparkSession.builder().appName("Solution1").master("local").getOrCreate()
    val users = sparkSession.read.schema(userSchema1).csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\clients_drivers.csv")
    val trips = sparkSession.read.option("dateFormat", "yyyy-mm-dd").schema(tripSchema1).csv("C:\\Users\\KrishnaChaitanyaDara\\Desktop\\Trips.csv")
    users.printSchema()
    users.show()
    trips.show()
    trips.printSchema()


    users.createTempView("usersTable")
    trips.createTempView("tripsTable")
    /*al innerjoin = users.col("_c0") === trips.col("_c1")
       users.join(trips,innerjoin).show()
      val innerjoin2 = users.col("_c0") === trips.col("_c2")
      users.join(trips,innerjoin2).show()*/

    /*  val temp1 = sparkSession.sql("select * from userstable" +
        " inner join tripstable on trim(userstable.userID) == trim(tripstable.Client_id)")
      temp1.createTempView("temp1table")
      val result1 = sparkSession.sql("select * ,userstable.userID as drivername from temp1table inner join userstable on temp1table.Driver_id == userstable.userID")
      result1.where("temp1table.banned NOT 'Yes' ").show()*/
    /*sparkSession.sql("select * from userstable u Full outer join  tripstable t on  u.userid == t.client_id  or  u.userid == t.driver_id").show()*/
    //    sparkSession.sql()
    /*sparkSession.sql("select userid ," +
      "(case when role  = 'client' then 'Yes'  end ) client, " +
      "(case when role = 'driver' then 'No' end) driver " +
      "from userstable " +
      "group by role").show()*/

    val driverdf = sparkSession.sql("select * from userstable where trim(role) == 'driver'")
    val clientdf = sparkSession.sql("select * from userstable where trim(role) == 'client'")
    driverdf.show()
    clientdf.show()
    driverdf.createTempView("driver")
    clientdf.createTempView("client")

    val result = sparkSession.sql("select *,d.banned as driver_banned ,c.banned as client_banned from tripstable t inner join driver d on trim(t.driver_id) == trim(d.userid)" +
      "inner join client c on  t.client_id == c.userid")
    import org.apache.spark.sql.functions._
    val result1 = result.where("trim(driver_banned) == 'No'").where("trim(client_banned) =='No'")
    //   .cube("Request_time","Status").agg(count("*"),grouping_id())

    val canclled = result1.groupBy("Request_time", "Status").count()
      .where("trim(Status) =='cancelled_by_driver' or trim(Status) =='cancelled_by_client'")
      .withColumnRenamed("count", "cancel_count")
      .withColumnRenamed("request_time","time")

    val total = result1.groupBy("Request_time").count().withColumnRenamed("count", "total_count")
    total.join(canclled, canclled("time") === total("request_time"), joinType = "Left")
      .withColumn("total_count",coalesce(col("total_count"),lit(0)))
      .withColumn("cancel_count",coalesce(col("cancel_count"),lit(0)))
      .selectExpr("request_time","cancel_count / total_count as percent" ).show()
      //.agg(col("cancel_count")/col("total_count")).show()

    //.groupBy("Request_time","Status").count().agg(expr("Status == 'cancelled_by_driver' or Status =='cancelled_by_client group by request_time'")).show()

    //result1.selectExpr("select status, count(status) from ")


    // agg(sum(expr("select count when trim(status) == 'cancelled_by_client' or trim(status) == 'cancelled_by_driver'"))/sum("count")).show()


    //  count().show()  //agg(count(expr("trim(Status) == 'cancelled_by_client' or  trim(status) == 'cancelled_by_driver'"))/count("Request_time")).show()


  }
}
