package notebook

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession,Row}

import notebook.{CustomerData, GeoLocationData, OrdersData, OrdersItemData, PaymentsData, ProductCategoryData, ProductsData, ReviewsData, SellerData}
import utills.CustomerUtills

object MainSparkDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Customer Data Project")
      .master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

      import spark.implicits._

    val schema = new StructType()
      .add("name", StringType)
      .add("department", StringType)
      .add("salary", IntegerType)


    val data = Seq(
      ("Alice", "HR", 50000),
      ("Bob", "null", 20000),
     ("null", "Sales", 60000)
    )

    val rdd = data.toDF("name","department","salary")
    val removednulldf = CustomerUtills.replaceStringNull(rdd)
    removednulldf.show()

    val customerDF = CustomerData.readCustomerData(spark, "csv", true)
    val geo_locDF = GeoLocationData.readLocatinData(spark, "csv", true)
    val order_itemDF = OrdersItemData.readOrderItemData(spark, "csv", true)
    val orderDF = OrdersData.readOrderData(spark, "csv", true)
    val paymentDF = PaymentsData.readPaymentData(spark, "csv", true)
    val productDF = ProductsData.readProductsData(spark, "csv", true)
    val categoryDF = ProductCategoryData.readCategoryData(spark, "csv", true)
    val reviewDF = ReviewsData.readReviewData(spark, "csv", true)
    val sellerDF = SellerData.readSellerData(spark, "csv", true)

//    reviewDF.show()

//    val trimDF = CustomerUtills.trimColumns(customerDF)
//    val replacenull = CustomerUtills.replaceStringNull(reviewDF)
//
//    GeoLocationData.write_file(geo_locDF)

//    customerDF.show()
//    geo_locDF.show()
//    orderDF.show()
//    order_itemDF.show()
//    paymentDF.show()
//    productDF.show()
//    categoryDF.show()
//    reviewDF.show()
//    sellerDF.show()







  }
}
