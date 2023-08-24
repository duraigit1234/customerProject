package scala.notebook

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import scala.utills.CustomerUtills

object GeoLocationData {

  val read_path = "src/main/resources/GeoLocation.csv"
  val write_path = "C:/SparkWorkFile/CustomerProject/output/"
  val customerSchema = new StructType()
    .add("geolocation_zip_code_prefix", IntegerType)
    .add("geolocation_lat", DoubleType)
    .add("geolocation_lang", DoubleType)
    .add("geolocation_city", StringType)
    .add("geolocation_state", StringType)

  def readLocatinData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, read_path, formate, customerSchema, header)
    df
  }

  def write_file(data:DataFrame)={
    CustomerUtills.writeFile(data,write_path)
  }

}
