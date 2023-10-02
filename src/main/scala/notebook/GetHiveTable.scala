package notebook

import org.apache.spark.sql.SparkSession

object GetHiveTable {

  def get_customer_table(spark:SparkSession,tableName:String,path:String):Unit={

    //customer
 spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
             |      customer_id, String,
             |      customer_unique_id, String,
             |      customer_zip_code_prefix, Int,
             |      customer_city, String,
             |      customer_state, String) STORED AS PARQUET
             |      LOCATION '$path'""".stripMargin)
  }
  def get_geoLocation_table(spark:SparkSession,tableName:String,path:String): Unit = {

    //location
 spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
             |      geolocation_zip_code_prefix, Int,
             |      geolocation_lat, Int,
             |      geolocation_lang, Int,
             |      geolocation_city, String,
             |      geolocation_state, String) STORED AS PARQUET
             |      LOCATION '$path'""".stripMargin)
  }
  def get_orders_table(spark:SparkSession,tableName:String,path:String): Unit = {


    //orders
    spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
                |      order_id, String
                |        customer_id, String
                |        order_status, String
                |        order_purchase_timestamp, String
                |        order_approved_at, String
                |        order_delivered_carrier_date, String
                |        order_deliver_customer_date, String
                |        order_estimated_delivery_date, String) STORED AS PARQUET
                |      LOCATION '$path'""".stripMargin)
  }

  def get_orderItem_table(spark:SparkSession,tableName:String,path:String): Unit = {


    //order item
   spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
               |      order_id, String,
               |      order_item, Int,
               |      product_id, String,
               |      seller_id, String,
               |      shipping_limit_date, String,
               |      price, Int,
               |      freight_value, Int) STORED AS PARQUET
               |      LOCATION '$path'""".stripMargin)

  }

  def get_product_table(spark:SparkSession,tableName:String,path:String): Unit = {

    //product
   spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
               |      product_category_name, String,
               |      product_id, String,
               |      product_name_length, Int,
               |      product_description_length, Int,
               |      product_photos_qty, Int,
               |      product_weight_g, Int,
               |      product_length_cm, Int,
               |      product_height_cm, Int,
               |      product_width_cm, Int) STORED AS PARQUET
               |      LOCATION '$path'""".stripMargin)

  }

  def get_category_table(spark:SparkSession,tableName:String,path:String): Unit = {

    //category
   spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
               |      product_category_name, String,
               |      product_category_name_english, String) STORED AS PARQUET
               |      LOCATION '$path'""".stripMargin)
  }

  def get_payment_table(spark:SparkSession,tableName:String,path:String): Unit = {

    //payment
   spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
               |      order_id, String,
               |      payment_sequential, Int,
               |      payment_type, String,
               |      payment_installments, Int,
               |      payment_value, Int) STORED AS PARQUET
               |      LOCATION '$path'""".stripMargin)

  }

  def get_review_table(spark:SparkSession,tableName:String,path:String): Unit = {
    //review
    spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
                |      review_id, StringType,
                |      order_id, String,
                |      review_score, Int,
                |      review_comment_title, String,
                |      review_comment_message, String,
                |      review_creation_date, String,
                |      review_answer_timestamp, String) STORED AS PARQUET
                |      LOCATION '$path'""".stripMargin)
  }

  def get_seller_table(spark:SparkSession,tableName:String,path:String): Unit = {

    //seller
    spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName(
                |      seller_id, String,
                |      seller_zip_code_prefix, Int,
                |      seller_city, String,
                |      seller_state, String) STORED AS PARQUET
                |      LOCATION '$path'""".stripMargin)
  }

}
