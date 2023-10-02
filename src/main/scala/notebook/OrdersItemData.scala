package notebook

import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utills.CustomerUtills

object OrdersItemData {

  val conf = ConfigFactory.load()
  val path = "src/main/resources/OrderItems.csv"
  val SourcePath = conf.getString("path.OrderItemsSourcePath")
  val Outputpath = conf.getString("path.OrderItemsOutputPath")
  val tableName = conf.getString("tablename.orderitem")
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("order_item", IntegerType)
    .add("product_id", StringType)
    .add("seller_id", StringType)
    .add("shipping_limit_date", StringType)
    .add("price", DoubleType)
    .add("freight_value", DoubleType)

  def readOrderItemData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_orderItem_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    val df1 = df.withColumn("shipping_limit_date",to_date(split(col("shipping_limit_date")," ").getItem(0),"M/dd/yyyy"))
//    CustomerUtills.writeFile(df1,Outputpath,"parquet")

  }

}
