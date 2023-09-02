package notebook

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions
import utills.CustomerUtills

import java.io.File

object CustomerData {

  val audit_table_path = "C://Users//SSLTP11358//Desktop//audit//audit_table.csv"
  val newfile = new File(audit_table_path)
  val exist_or_not = newfile.exists()
  val path = "src/main/resources/Customers.csv"
  val SourcePath = "/user/cloudera/data/source_data/customers"
  val Outputpath = "/user/cloudera/data/staging/customers"

  val audit_table_schema = new StructType()
    .add("Application_ID", StringType)
    .add("Job_Run_Date", StringType)
    .add("Source_Path", StringType)
    .add("Target_Path", StringType)
    .add("Table_Name", StringType)
    .add("Source_Count", IntegerType)
    .add("Target_Count", IntegerType)
    .add("Application_Status", StringType)
    .add("Load_Type", StringType)
    .add("Snap_Shot_Date", StringType)

  val emptySchema = List(("name", "string"), ("age", "int"), ("salary", "int"))


  //  val audit_table_schema = Seq("Application_ID","Job_Run_Date","Source_Path")

  val customerSchema = new StructType()
    .add("customer_id", StringType)
    .add("customer_unique_id", StringType)
    .add("customer_zip_code_prefix", IntegerType)
    .add("customer_city", StringType)
    .add("customer_state", StringType)

  def readCustomerData(spark: SparkSession, formate: String, header: Boolean): Unit = {

    val appid = spark.sparkContext.applicationId

    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header).withColumn("load_dtm",current_timestamp())
    val dfcount = df.count()
    if (exist_or_not){
        val audit_df = spark.read.option("header",true).format("csv").load(audit_table_path)
      audit_df.show()
    }
    else {

      val df = spark.createDataFrame(spark.sparkContext
        .emptyRDD[Row], audit_table_schema)

      val seqdata = Seq(
        (appid,current_date(),SourcePath,Outputpath,"Customers",dfcount,100,"success","full load")
      )

      df.write.csv(audit_table_path)


    }

//    CustomerUtills.writeFile(df,Outputpath)

  }

}
