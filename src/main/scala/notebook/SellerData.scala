package scala.notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.utills.CustomerUtills
object SellerData {

  val path = "src/main/resources/Sellers.csv"
  val customerSchema = new StructType()
    .add("seller_id", StringType)
    .add("seller_zip_code_prefix", IntegerType)
    .add("seller_city", StringType)
    .add("seller_state", StringType)


  def readSellerData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    df
  }

}
