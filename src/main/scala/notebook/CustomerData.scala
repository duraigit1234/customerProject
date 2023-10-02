package notebook

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions
import utills.CustomerUtills
import notebook.GetHiveTable
import java.io.File
import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable

object CustomerData {
  val conf = ConfigFactory.load()

  val audit_table_path = "C://Users//SSLTP11358//Desktop//audit//audit_table.csv"
  val newfile = new File(audit_table_path)
  val exist_or_not = newfile.exists()
  val path = "src/main/resources/Customers.csv"
  val SourcePath = conf.getString("path.CustomerSourcePath")
  val Outputpath = conf.getString("path.CustomerOutputPath")
  val tableName = conf.getString("tablename.customer")

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
      GetHiveTable.get_customer_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)

//    CustomerUtills.writeFile(df,Outputpath)

  }

}
