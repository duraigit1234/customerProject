package scala.notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.utills.CustomerUtills

object OrdersItemData {

  val path = "src/main/resources/OrderItems.csv"
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("order_item", IntegerType)
    .add("product_id", StringType)
    .add("seller_id", StringType)
    .add("shipping_limit_date", StringType)
    .add("price", DoubleType)
    .add("freight_value", DoubleType)

  def readOrderItemData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    val df1 = df.withColumn("shipping_limit_date",to_date(split(col("shipping_limit_date")," ").getItem(0),"M/dd/yyyy"))
    df1
  }

}
