package utills

import org.apache.spark.sql.types.{StringType,StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CustomerUtills {

//  val stringColumn = org.apache.spark.sql.types.StringType

  def readFile(spark: SparkSession, path: String, formate: String, schema: StructType, header: Boolean): DataFrame = {

    try {

      val df = spark.read.format(formate).schema(schema)
        .option("header", header)
//        .option("timestampFormat", "yyyy-MM-dd HH:mm")
        .load(path)

      if (df.isEmpty) {
        println("The DataFrame is empty.")
        spark.emptyDataFrame
      } else {
        df
      }
    } catch {
      case ex: java.io.FileNotFoundException =>
        println(s"FileNotFoundException: ${ex.getMessage}")
        spark.emptyDataFrame
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
        spark.emptyDataFrame
    }


  }

  def writeFile(data:DataFrame,path:String)={

    try {
      if (data.isEmpty) {
        println("The DataFrame is empty.")
      } else {
        //data.write.format("parquet").save(path)
        data.write.format("parquet").save(path)
        println("Data written successfully.")
      }

    } catch {
      case ex: java.io.IOException =>
        println(s"IOException: ${ex.getMessage}")
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
    }
  }

  def replaceStringNull(data:DataFrame):DataFrame={
    val stringColumns = data.columns.filter(
      (colm)=>
        data.schema(colm).dataType == org.apache.spark.sql.types.StringType
    )

    println("String columns : "+stringColumns.mkString(","))

    val df = stringColumns.foldLeft(data)((df, colName) =>
//      df.withColumn(colName,when(col(colName) === null,"hello").otherwise(col(colName)))
      df.na.replace(colName,Map("null" ->"Hello"))
    )
    df
  }

  def trimColumns(data:DataFrame):DataFrame={
    val df = data.columns.foldLeft(data){
      (newdf,colmn)=>
          newdf.withColumn(colmn,trim(col(colmn)))
    }
    df
  }

}
