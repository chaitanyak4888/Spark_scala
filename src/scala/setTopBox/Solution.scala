package setTopBox

import org.apache.spark.sql.SparkSession

object Solution {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("setTopbox").getOrCreate()

  }

}
