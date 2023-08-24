package scala.notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.utills.CustomerUtills

object OrdersData {

  val path = "src/main/resources/Orders.csv"
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("customer_id", StringType)
    .add("order_status", StringType)
    .add("order_purchase_timestamp", StringType)
    .add("order_approved_at", StringType)
    .add("order_delivered_carrier_date", StringType)
    .add("order_deliver_customer_date", StringType)
    .add("order_estimated_delivery_date", StringType)

  def readOrderData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    val df1 = df.withColumn("order_purchase_timestamp",to_date(split(col("order_purchase_timestamp")," ").getItem(0),"M/dd/yyyy"))
                .withColumn("order_approved_at",to_date(split(col("order_approved_at")," ").getItem(0),"M/dd/yyyy"))
                .withColumn("order_delivered_carrier_date",to_date(split(col("order_delivered_carrier_date")," ").getItem(0),"M/dd/yyyy"))
                .withColumn("order_deliver_customer_date",to_date(split(col("order_deliver_customer_date")," ").getItem(0),"M/dd/yyyy"))
                .withColumn("order_estimated_delivery_date",to_date(split(col("order_estimated_delivery_date")," ").getItem(0),"M/dd/yyyy"))

//    df1.show()
    df
  }

}
