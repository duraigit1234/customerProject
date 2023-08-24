package scala.notebook

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.utills.CustomerUtills

object PaymentsData {

  val path = "src/main/resources/OrderPayments.csv"
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("payment_sequential", IntegerType)
    .add("payment_type", StringType)
    .add("payment_installments", IntegerType)
    .add("payment_value", DoubleType)

  def readPaymentData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    df
  }

}
