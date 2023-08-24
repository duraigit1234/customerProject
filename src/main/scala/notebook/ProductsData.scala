package scala.notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.utills.CustomerUtills
object ProductsData {

  val path = "src/main/resources/Products.csv"
  val customerSchema = new StructType()
    .add("product_id", StringType)
    .add("product_category_name", StringType)
    .add("product_name_length", IntegerType)
    .add("product_description_length", IntegerType)
    .add("product_photos_qty", IntegerType)
    .add("product_weight_g", IntegerType)
    .add("product_length_cm", IntegerType)
    .add("product_height_cm", IntegerType)
    .add("product_width_cm", IntegerType)


  def readProductsData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    df
  }

}
