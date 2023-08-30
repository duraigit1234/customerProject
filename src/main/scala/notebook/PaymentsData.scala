package notebook

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utills.CustomerUtills

object PaymentsData {

  val path = "src/main/resources/OrderPayments.csv"
  val SourcePath = "/user/cloudera/data/source_data/order_payments"
  val Outputpath = "/user/cloudera/data/staging/order_payments"
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("payment_sequential", IntegerType)
    .add("payment_type", StringType)
    .add("payment_installments", IntegerType)
    .add("payment_value", DoubleType)

  def readPaymentData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    CustomerUtills.writeFile(df,Outputpath)

  }

}
