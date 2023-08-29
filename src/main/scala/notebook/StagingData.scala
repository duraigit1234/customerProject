package notebook

import org.apache.spark.sql.DataFrame
import notebook.{CustomerData, GeoLocationData, OrdersData, OrdersItemData, PaymentsData, ProductCategoryData, ProductsData, ReviewsData, SellerData}
import utills.CustomerUtills
import org.apache.spark.sql.SparkSession
object StagingData {

  val output_path = "/user/cloudera/data/consolidated/order_information"

  def get_result_table(spark:SparkSession):Unit={
    val customerDF = CustomerData.readCustomerData(spark, "csv", true)
    val geo_locDF = GeoLocationData.readLocatinData(spark, "csv", true)
    val order_itemDF = OrdersItemData.readOrderItemData(spark, "csv", true)
    val orderDF = OrdersData.readOrderData(spark, "csv", true)
    val paymentDF = PaymentsData.readPaymentData(spark, "csv", true)
    val productDF = ProductsData.readProductsData(spark, "csv", true)
    val categoryDF = ProductCategoryData.readCategoryData(spark, "csv", true)
    val reviewDF = ReviewsData.readReviewData(spark, "csv", true)
    val sellerDF = SellerData.readSellerData(spark, "csv", true)

    val joinsdata = orderDF.join(customerDF,orderDF("customer_id") === customerDF("customer_id"))
      .join(order_itemDF,orderDF("order_id") === order_itemDF("order_id"))
      .join(productDF,order_itemDF("product_id") === productDF("product_id"))
      .join(paymentDF,orderDF("order_id") === paymentDF("order_id"))
      .join(reviewDF,reviewDF("order_id") === orderDF("order_id"))


  val result =  joinsdata.select(
      orderDF("order_id"),
      customerDF("customer_id"),customerDF("customer_unique_id"),customerDF("customer_city"),customerDF("customer_state"),
      productDF("product_category_name"), productDF("product_name_length"),
      order_itemDF("order_item"),order_itemDF("product_id"),order_itemDF("shipping_limit_date"),order_itemDF("price"),order_itemDF("freight_value"),
      paymentDF("payment_value"),
      reviewDF("review_score")
    )

    result.show()

    CustomerUtills.writeFile(result,output_path)

  }

}
