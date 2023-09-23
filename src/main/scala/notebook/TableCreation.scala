package notebook

import org.apache.spark.sql.SparkSession

object TableCreation {

  def productTableCreation(spark: SparkSession,path: String,tableName: String): Unit ={

    spark.sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (product_id string,
                |product_category_name string,
                |product_name_length int,
                |product_description_length int,
                |product_photos_qty int,
                |product_weight_g int,
                |product_length_cm int,
                |product_height_cm int,
                |product_width_cm int)
                |STORED AS PARQUET
                |location '$path' """.stripMargin)

  }

}

