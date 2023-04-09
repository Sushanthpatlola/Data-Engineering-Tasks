package pack


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io._

object obj {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val html = Source.fromURL("https://randomuser.me/api/0.8/?results=100")
    val urldata = html.mkString
    println(urldata)

    val urlrdd = sc.parallelize(List(urldata))

    val df = spark.read.json(urlrdd)
    df.show()

    df.printSchema()

    val flattendf = df.withColumn("results", expr("explode(results)"))
    flattendf.show()
    flattendf.printSchema()

    val flattendf1 = flattendf.select(
      "nationality",
      "results.user.*",
      "results.user.location.*",
      "results.user.name.*",
      "results.user.picture.*",
      "seed",
      "version")
      .drop("results")
      .drop("location")
      .drop("name")
      .drop("picture")

    flattendf1.show()

    flattendf1.printSchema()

    val alist = Seq[String]("nationality", "cell", "dob", "email", "gender",
      "md5", "phone", "password",
      "registered", "salt", "sha1", "sha256", "username",
      "city", "state", "street", "zip",
      "first", "last", "title", "large", "medium",
      "thumbnail", "seed", "version")
    val colNames = alist.map(names => col(names))

    val flattendf2 = flattendf1.select(colNames: _*)

    flattendf2.show()
    flattendf2.printSchema()

    println("==Removing Numbers in Username==")

    val rmvnum = flattendf2.withColumn(
      "username",
      regexp_replace(col("username"), "([0-9])", ""))
    rmvnum.show()

    println("===AVRO READ ===")
    val avrodf = spark.read.format("avro").option("header", "true")
      .load("file:///D:/data/projectsample.avro")
    avrodf.show()

    val joindf = avrodf.join(broadcast(rmvnum), Seq("username"), "left")
    println("==Broadcast left Join==")
    joindf.show()

    val joindfnull = joindf.filter(col("nationality").isNull)
    println("==Nationality Null Values==")
    joindfnull.show()

    val joindfnotnull = joindf.filter(col("nationality").isNotNull)
    println("==Nationality Not Null Values==")
    joindfnotnull.show()

    val replace_null = joindfnull.na.fill("Not Available").na.fill(0)
    println("==Replaced Null Values==")
    replace_null.show()

    val replace_null_current_date = replace_null
      .withColumn("Current_date", expr("date_format(current_timestamp(),'yyyy-MM-dd')"))
    println("===Replace null with Current Date===")
    replace_null_current_date.show()

    val not_null_current_date = replace_null
      .withColumn("Current_date", expr("date_format(current_timestamp(),'yyyy-MM-dd')"))
    println("===Not null with Current Date===")
    not_null_current_date.show()

      replace_null_current_date.write.format("parquet").mode("append").partitionBy("Current_date")
                               .save("file:///D:/data/project/availablecustomers")
                               
      not_null_current_date.write.format("parquet").mode("append").partitionBy("Current_date")
                               .save("file:///D:/data/project/availablecustomers")
     println("==Data Written==")
     
  }

}
