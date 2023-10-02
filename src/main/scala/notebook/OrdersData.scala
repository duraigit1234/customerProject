package notebook


import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import utills.CustomerUtills
import org.apache.hadoop.fs.{FileSystem, Path}
import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable

import java.io.File

object OrdersData {

  val conf = ConfigFactory.load()


  val audit_table_path = conf.getString("path.audit_table_path")
  val SourcePath = conf.getString("path.SourcePath")
  val OutputPath = conf.getString("path.OutputPath")
  val tableName = conf.getString("tablename.orders")
  val csvformate = conf.getString("dataformate.csvformate")
  val parquetformate = conf.getString("dataformate.parquetformate")


  val newfile = new File(audit_table_path)
  val exist_or_not = newfile.exists()

  val audit_table_schema = new StructType()
    .add("Application_ID", StringType)
    .add("Job_Run_Date", StringType)
    .add("Source_Path", StringType)
    .add("Target_Path", StringType)
    .add("Table_Name", StringType)
    .add("Source_Count", LongType)
    .add("Target_Count", LongType)
    .add("Application_Status", StringType)
    .add("Load_Type", StringType)
    .add("Snap_Shot_Date", StringType)

  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("customer_id", StringType)
    .add("order_status", StringType)
    .add("order_purchase_timestamp", StringType)
    .add("order_approved_at", StringType)
    .add("order_delivered_carrier_date", StringType)
    .add("order_deliver_customer_date", StringType)
    .add("order_estimated_delivery_date", StringType)

  def readOrderData(spark: SparkSession, formate: String, header: Boolean): Unit = {
  GetHiveTable.get_orders_table(spark,tableName,OutputPath)
//    val exist_or_not = testDirExist(spark,audit_table_path)
    val appid = spark.sparkContext.applicationId
        if (exist_or_not){
         val existing_audit = CustomerUtills.readFile(spark,audit_table_path,csvformate,audit_table_schema,true)
          val table_exist_or_not = existing_audit.filter(col("Table_Name") === "orders").count() > 0

          if (table_exist_or_not){
            val windspec = Window.partitionBy(existing_audit("Table_Name")).orderBy(existing_audit("Snap_Shot_Date").desc)
            val rownum = existing_audit.withColumn("row",row_number().over(windspec))
              .where(col("Table_Name") === "orders" and col("row") === 1)

            val latest_load_date = rownum.select("Snap_Shot_Date").collect()(0)(0).toString

            println(s"latest load Date $latest_load_date")
            val filterdf = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
            val   trans_fiterdata = transform_data(filterdataframe(filterdf,latest_load_date))
            try {
              val newmaxDateDF = trans_fiterdata.agg(max("order_purchase_timestamp").alias("maxDate"))
              val newmaxDateAsString = newmaxDateDF.collect()(0)(0).toString
              println(s"Maximum Date as String: $newmaxDateAsString")
              val newdatetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
              val newcurrentDateTimeAsString = newdatetimedf.collect()(0)(0).toString

              val status = CustomerUtills.writeFile(trans_fiterdata, OutputPath, "append", csvformate)
              val newcnt = trans_fiterdata.count()
              val seqdata = Seq(
                (appid, newcurrentDateTimeAsString, SourcePath, OutputPath, "orders", newcnt, newcnt, status, "incremental load", newmaxDateAsString)
              )
              val newauditDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames: _*)

              CustomerUtills.writeFile(newauditDF, audit_table_path, "append", csvformate)
            } catch {
              case _: Throwable =>
                println("No data available")
            }

          }
          else {
            val df = CustomerUtills.readFile(spark, SourcePath, csvformate, customerSchema, header)
            val df1 = transform_data(df)

            val maxDateDF = df1.agg(max("order_purchase_timestamp").alias("maxDate"))
            val maxDateAsString = maxDateDF.collect()(0)(0).toString

            val datetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
            val currentDateTimeAsString = datetimedf.collect()(0)(0).toString

          val status =  CustomerUtills.writeFile(df1, OutputPath, "overwrite", csvformate)
            val cnt = df1.count()
            val seqdata = Seq(
              (appid, currentDateTimeAsString, SourcePath, OutputPath, "orders", cnt, cnt, status, "full load", maxDateAsString)
            )
            val emptyDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames: _*)

            CustomerUtills.writeFile(emptyDF, audit_table_path, "append", csvformate)
          }

        }
        else{

          val df = CustomerUtills.readFile(spark, SourcePath, csvformate, customerSchema, header)
          val df1 = transform_data(df)

          val maxDateDF = df1.agg(max("order_purchase_timestamp").alias("maxDate"))
          val maxDateAsString = maxDateDF.collect()(0)(0).toString

            val datetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
            val currentDateTimeAsString =datetimedf.collect()(0)(0).toString


          val status = CustomerUtills.writeFile(df1, OutputPath, "overwrite", csvformate)
          val cnt = df1.count()
          val seqdata = Seq(
            (appid,currentDateTimeAsString,SourcePath,OutputPath,"orders",cnt,cnt,status,"full load",maxDateAsString)
          )
        val emptyDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames:_*)

          CustomerUtills.writeFile(emptyDF,audit_table_path,"append",csvformate)
        }


  }

  def testDirExist(spark:SparkSession,path: String): Boolean = {
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  def transform_data(data:DataFrame):DataFrame={
    val df1 = data.withColumn("order_purchase_timestamp", to_date(split(col("order_purchase_timestamp"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_approved_at", to_date(split(col("order_approved_at"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_delivered_carrier_date", to_date(split(col("order_delivered_carrier_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_deliver_customer_date", to_date(split(col("order_deliver_customer_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_estimated_delivery_date", to_date(split(col("order_estimated_delivery_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("load_dtm", current_timestamp())
    df1
  }
  def filterdataframe(df:DataFrame,filtercond:String):DataFrame={
    println("inside filtered dataframe")
    val filterdate = to_date(lit(filtercond))
    println(s"max date of audit table $filterdate")
    val result = df.filter(to_date(split(col("order_purchase_timestamp"), " ").getItem(0), "M/dd/yyyy") > filterdate)
    result.show()
    result
  }

}
