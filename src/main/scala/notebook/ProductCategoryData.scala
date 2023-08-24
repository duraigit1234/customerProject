package scala.notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.utills.CustomerUtills

object ProductCategoryData {

  val path = "src/main/resources/ProductCategory.csv"
  val customerSchema = new StructType()
    .add("product_category_name", StringType)
    .add("product_category_name_english", StringType)


  def readCategoryData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    df
  }

}
