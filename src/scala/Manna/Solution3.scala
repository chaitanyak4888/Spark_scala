package Manna

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Solution3 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    val spark = SparkSession.builder().appName("Solution3").master("local").getOrCreate()

    val empSchema :StructType = StructType(List(
      StructField("Id",StringType,false),
      StructField("Name",StringType,false),
      StructField("Salary",StringType,false),
      StructField("DepartmentId",StringType,false)))

    val deptSchema : StructType = StructType(List(
      StructField("deptId",IntegerType,false),
      StructField("DeptName",StringType,false)))

    val dept = spark.read.schema(deptSchema)csv("input/department.csv")
    val emp = spark.read.schema(empSchema)csv("input/employee.csv")
    val emp1 = emp.withColumn("DepartmentId",trim(col("DepartmentId")))
    emp.printSchema()
    emp1.show()
    dept.show()
    dept.printSchema()


    emp1.join(dept,emp1("DepartmentId") === dept("DepartmentId"),"inner").orderBy("Id").show()

  }
}
