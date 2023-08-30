package notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utills.CustomerUtills

object OrdersItemData {

  val path = "src/main/resources/OrderItems.csv"
  val SourcePath = "/user/cloudera/data/source_data/order_items"
  val Outputpath = "/user/cloudera/data/staging/order_items"
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("order_item", IntegerType)
    .add("product_id", StringType)
    .add("seller_id", StringType)
    .add("shipping_limit_date", StringType)
    .add("price", DoubleType)
    .add("freight_value", DoubleType)

  def readOrderItemData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    val df1 = df.withColumn("shipping_limit_date",to_date(split(col("shipping_limit_date")," ").getItem(0),"M/dd/yyyy"))
    CustomerUtills.writeFile(df1,Outputpath)

  }

}
