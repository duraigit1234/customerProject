package notebook

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession,Row}

import notebook.{CustomerData, GeoLocationData, OrdersData, OrdersItemData, PaymentsData, ProductCategoryData, ProductsData, ReviewsData, SellerData}
import utills.CustomerUtills
import notebook.StagingData
import com.typesafe.config.ConfigFactory

object MainSparkDriver {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("application.conf")
    val appname = conf.getString("spark.appname")
    val master = conf.getString("spark.master")

    val spark = SparkSession.builder()
      .appName(appname)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")


//    CustomerData.readCustomerData(spark, "csv", true)
//    GeoLocationData.readLocatinData(spark, "csv", true)
//    OrdersItemData.readOrderItemData(spark, "csv", true)
    println("OrdersData Going to Call")
//    OrdersData.readOrderData(spark, "csv", true)
//    PaymentsData.readPaymentData(spark, "csv", true)
    ProductsData.readProductsData(spark, "csv", true)
//    ProductCategoryData.readCategoryData(spark, "csv", true)
//    ReviewsData.readReviewData(spark, "csv", true)
//    SellerData.readSellerData(spark, "csv", true)




  }
}
