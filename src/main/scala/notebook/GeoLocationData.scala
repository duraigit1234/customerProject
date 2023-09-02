package notebook

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import utills.CustomerUtills

object GeoLocationData {

  val path = "src/main/resources/GeoLocation.csv"
  val SourcePath = "/user/cloudera/data/source_data/geolocations"
  val Outputpath = "/user/cloudera/data/staging/geolocations"
  val customerSchema = new StructType()
    .add("geolocation_zip_code_prefix", IntegerType)
    .add("geolocation_lat", DoubleType)
    .add("geolocation_lang", DoubleType)
    .add("geolocation_city", StringType)
    .add("geolocation_state", StringType)

  def readLocatinData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }


}
