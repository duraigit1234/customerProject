package notebook

import org.apache.spark.sql.DataFrame
import notebook.{CustomerData, GeoLocationData, OrdersData, OrdersItemData, PaymentsData, ProductCategoryData, ProductsData, ReviewsData, SellerData}
import utills.CustomerUtills
import org.apache.spark.sql.SparkSession
object StagingData {

  val output_path = "/user/cloudera/data/consolidated/order_information"

  val order_payment_path = "/user/cloudera/data/staging/order_payments"
  val customers_path = "/user/cloudera/data/staging/customers"
  val geo_location_path = "/user/cloudera/data/staging/geolocations"
  val orders_path = "/user/cloudera/data/staging/orders"
  val order_item_path = "/user/cloudera/data/staging/order_items"
  val product_category_path = "/user/cloudera/data/staging/product_category_name_translation"
  val product_path = "/user/cloudera/data/staging/products"
  val review_path = "/user/cloudera/data/staging/order_reviews"
  val seller_path = "/user/cloudera/data/staging/sellers"

  def get_result_table(spark:SparkSession):Unit={

    val customerDF = CustomerUtills.readFile(spark, customers_path,"parquet")
    val geo_locDF = CustomerUtills.readFile(spark, geo_location_path,"parquet")
    val order_itemDF = CustomerUtills.readFile(spark, order_item_path,"parquet")
    val orderDF = CustomerUtills.readFile(spark, orders_path,"parquet")
    val paymentDF = CustomerUtills.readFile(spark, order_payment_path,"parquet")
    val productDF = CustomerUtills.readFile(spark, product_path,"parquet")
    val categoryDF = CustomerUtills.readFile(spark, product_category_path,"parquet")
    val reviewDF = CustomerUtills.readFile(spark, review_path,"parquet")
    val sellerDF = CustomerUtills.readFile(spark, seller_path,"parquet")

    val joinsdata = orderDF.join(customerDF,orderDF("customer_id") === customerDF("customer_id"))
      .join(order_itemDF,orderDF("order_id") === order_itemDF("order_id"))
      .join(productDF,order_itemDF("product_id") === productDF("product_id"))
      .join(paymentDF,orderDF("order_id") === paymentDF("order_id"))
      .join(reviewDF,reviewDF("order_id") === orderDF("order_id"))
      .select(
      orderDF("order_id"),
      customerDF("customer_id"), customerDF("customer_unique_id"), customerDF("customer_city"), customerDF("customer_state"),
      productDF("product_category_name"), productDF("product_name_length"),
      order_itemDF("order_item"), order_itemDF("product_id"), order_itemDF("shipping_limit_date"), order_itemDF("price"), order_itemDF("freight_value"),
      paymentDF("payment_value"),
      reviewDF("review_score")
    )

//    CustomerUtills.writeFile(joinsdata,output_path,"parquet")

  }

}
