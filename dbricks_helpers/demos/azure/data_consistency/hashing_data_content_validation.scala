// Databricks notebook source
// DBTITLE 1,Set up Spark Session
// Setup the Spark Session to communicate with FS (ADL, HDL)
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{struct, col, lit, concat_ws, sha2, collect_list, coalesce}
import java.security.MessageDigest
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel._

spark.sparkContext.hadoopConfiguration.set("fs.hdlfs.impl", "com.sap.hana.datalake.files.HdlfsFileSystem")
spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.hdlfs.impl", "com.sap.hana.datalake.files.Hdlfs")
spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

spark.sparkContext.hadoopConfiguration.set("fs.hdlfs.operation.open.mode", "DEFAULT")
spark.sparkContext.hadoopConfiguration.set("fs.hdlfs.ssl.keystore.type", "pkcs12")

// Configure Spark session Hadoop Config for HDL 
spark.sparkContext.hadoopConfiguration.set("fs.hdlfs.ssl.keystore.location", "/dbfs/FileStore/cdf-hdl/********.p12")
spark.sparkContext.hadoopConfiguration.set("fs.hdlfs.ssl.keystore.password", "")
spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdlfs://***************.files.hdl.prod-us21.hanacloud.ondemand.com")

// Configure Spark Session Hadoop Config for ADL 
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.name", "**********")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key", "*************")

// COMMAND ----------

// DBTITLE 1,Notebook Parameters
// Data serialization method
var serialize_method = "xxhash"//"sha"

// Base path for CDF archive
val cdf_archive_base_path = "hdlfs://************.files.hdl.prod-us21.hanacloud.ondemand.com/cdf-data-archive"
// Base path for DI archive
val di_archive_base_path = "abfs://**********@insightstoragedev.dfs.core.windows.net/cornerstone-data-archive/"

// CDF table name to retrieve
val cdf_table_name = "*************"
// DI table name to retrieve
val di_table_name = "***************"

// COMMAND ----------

import java.security.MessageDigest
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import net.jpountz.xxhash.XXHashFactory
import java.nio.ByteBuffer

// Define Spark UDF to hash the content of a DF Row 
val serialize_data = udf((row: Row, serializationType: String) => {
  val rowStr = row.json
  val hashObject = serializationType match {
    case "md5" => MessageDigest.getInstance("MD5").digest(rowStr.getBytes)
    case "sha" => MessageDigest.getInstance("SHA-256").digest(rowStr.getBytes)
    case "blake3" => MessageDigest.getInstance("BLAKE3").digest(rowStr.getBytes)
    case "crypto" =>
      val cryptObject = MessageDigest.getInstance("SHA-256")
      cryptObject.update(rowStr.getBytes)
      cryptObject.digest()
    case "complexjson" =>
      MessageDigest.getInstance("SHA-256").digest(rowStr.getBytes)
    case "xxhash" =>
      val factory = XXHashFactory.fastestInstance()
      val inputBytes = rowStr.getBytes("UTF-8")
      val seed = 0 // Seed for the hash function, you can choose any integer
      val hash64 = factory.hash64()
      val xxHashValue = hash64.hash(inputBytes, 0, inputBytes.length, seed)
      
      // Convert xxHashValue to bytes
      ByteBuffer.allocate(java.lang.Long.BYTES).putLong(xxHashValue).array()
      
    case _ => throw new IllegalArgumentException(s"Invalid serialization type: $serializationType")
  }
  hashObject
})

// COMMAND ----------

// DBTITLE 1,Generic Data Consistency Helper Functions
// Caching hash results
def serializeDataFrame(df: DataFrame, serializationType: String): DataFrame = {
  df.withColumn(serializationType, serialize_data(struct(df.columns.map(col): _*), lit(serializationType)))
}

// Find the optimal number of partitions for a Spark df
def calculateOptimalPartitions(df: DataFrame, partitionSize: Long): Int = {
  // Calculate the total number of records in the DataFrame
  val totalRecords = df.count()
  
  // Calculate the number of partitions based on the total number of records and the desired partition size
  val numPartitions = Math.ceil(totalRecords.toDouble / partitionSize).toInt
  numPartitions
}

// Print the number of partition in Spark df
def printNumPartitions(df: DataFrame): Unit = {
  // Read the number of partitions
  val numPartitions = df.rdd.partitions.length
  println(s"Number of partitions: $numPartitions")
}

// COMMAND ----------

// DBTITLE 1,Function to Get HDLFS and ADLS Table and File Paths
def getTablePaths(basePath: String): Map[String, String] = {
  // List files in the directory
  val files = dbutils.fs.ls(basePath)

  // Create an empty mutable Map to store table names and paths
  val tablePaths = Map[String, String]()

  // Iterate over the list of files and store each file's details in the map
  files.foreach { fileInfo =>
    val tableName = if (fileInfo.name.endsWith("/")) fileInfo.name.dropRight(1) else fileInfo.name
    tablePaths.put(tableName, fileInfo.path)
  }

  // Return the map containing table names and paths
  tablePaths
}

// COMMAND ----------

// DBTITLE 1,Get all HDLFS and ADLS Gen2 Tables and Table Paths
// Call the function to get all the table names and table paths for CDF archive
val cdf_archive_table_paths = getTablePaths(cdf_archive_base_path)

// Call the function to get all the table names and table paths for DI archive
val di_archive_table_paths = getTablePaths(di_archive_base_path)

// COMMAND ----------

// DBTITLE 1,Read a CDF Archive Table into a Spark Dataframe
// Retrieve the table path from the map
val cdf_table_path = cdf_archive_table_paths.getOrElse(cdf_table_name, "Table not found")
// Read the data in to a Spark dataframe
val cdf_archive_table_df = spark.read.load(cdf_table_path)
println(s"cdf_archive_table_df count: ${cdf_archive_table_df.count()}")

// Repartition CDF archive
val partitionSize = 500000  // Desired size of each partition
val numPartitions = calculateOptimalPartitions(cdf_archive_table_df, partitionSize)
val cdf_archive_table_df_partitioned = cdf_archive_table_df.repartition(numPartitions)

// COMMAND ----------

// DBTITLE 1,Read a DI Archive Table into a Spark Dataframe
// Retrieve the table path from the map
val di_table_path = di_archive_table_paths.getOrElse(di_table_name, "Table not found")
// Read the data in to a Spark dataframe
val di_archive_table_df = spark.read.format("parquet").load(di_table_path)
println(s"di_archive_table_df count: ${di_archive_table_df.count()}")

// Repartition DI archive
val partitionSize = 500000  // Desired size of each partition
val numPartitions = calculateOptimalPartitions(di_archive_table_df, partitionSize)
val di_archive_table_df_partitioned = di_archive_table_df.repartition(numPartitions)

// COMMAND ----------

// DBTITLE 1,Apply Data Serialization Hashing Techniques (CDF)
val cdf_archive_df_serialized = serializeDataFrame(cdf_archive_table_df_partitioned, serialize_method)
  .select(serialize_method)
  .orderBy(serialize_method)
  .persist(MEMORY_ONLY) //MEMORY_AND_DISK_SER
cdf_archive_df_serialized.count()

// COMMAND ----------

// DBTITLE 1,Apply Data Serialization Hashing Techniques (DI)
val di_archive_df_serialized = serializeDataFrame(di_archive_table_df_partitioned, serialize_method)
  .select(serialize_method)
  .orderBy(serialize_method)
  .persist(MEMORY_ONLY) //MEMORY_AND_DISK_SER
di_archive_df_serialized.count()

// COMMAND ----------

// DBTITLE 1,Compare Serialization Keys For Data Consistency Match
// Perform a full outer join on the "rowHash" column
val cdf_di_joined = cdf_archive_df_serialized.join(di_archive_df_serialized, Seq(serialize_method), "full_outer")

val cdf_hashes_not_in_di = cdf_di_joined
  .filter(di_archive_df_serialized(serialize_method).isNull)
  .withColumn("source", lit("cdf_archive"))
  .withColumn("table_name", lit(cdf_table_name))

// Find di hashes which are not in cdf
val di_hashes_not_in_cdf = cdf_di_joined
  .filter(cdf_archive_df_serialized(serialize_method).isNull)
  .withColumn("source", lit("di_archive"))
  .withColumn("table_name", lit(di_table_name))

// COMMAND ----------

// DBTITLE 1,Output the Data Serialization Results
// compare two serialized columns between both dataframes (left anti join)// Show the results
if (cdf_hashes_not_in_di.count() > 0 || di_hashes_not_in_cdf.count() > 0) {
    val error_message = "the raw data between CDF Publish and DI Landing Zone does NOT match.... "
    throw new Exception(error_message)
} else {
    println("the raw data is exactly the same between CDF Publish and the DI Landing Zone...")
}

// COMMAND ----------

cdf_hashes_not_in_di.count()

// COMMAND ----------

di_hashes_not_in_cdf.count()
