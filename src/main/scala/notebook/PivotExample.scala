package notebook

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PivotExample {

  def main(args: Array[String]): Unit = {

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    val spark = SparkSession.builder().master("yarn").appName("pivotExample").getOrCreate()

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    df.groupBy("product").pivot("country").sum("amount").show()

  }

}
