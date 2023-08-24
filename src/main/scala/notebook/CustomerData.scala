package scala.notebook

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.utills.CustomerUtills

object CustomerData {

  val path = "src/main/resources/Customers.csv"
  val customerSchema = new StructType()
    .add("customer_id", StringType)
    .add("customer_unique_id", StringType)
    .add("customer_zip_code_prefix", IntegerType)
    .add("customer_city", StringType)
    .add("customer_state", StringType)

  def readCustomerData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    df
  }

}
