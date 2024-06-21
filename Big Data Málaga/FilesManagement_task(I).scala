// Databricks notebook source
import org.apache.spark.sql.DataFrame


val df: DataFrame = spark.createDataFrame(
                                          Seq(
                                              ("1", "6245607513", "Malaga", "0386", "21874301P", "2023-05-12"),
                                              ("2", "1283973874", "Madrid", "2287", "21874301P", "2023-05-12"),
                                              ("3", "0987252925", "Valencia", "0872", "21874301P", "2023-05-12"),
                                              ("4", "2937638976", "Bilbao", "2847", "21874301P", "2023-05-12"),
                                              ("5", "7239657623", "Barcelona", "1038", "21874301P", "2023-05-12"),
                                              ("6", "2786527574", "A Coruña", "0983", "21874301P", "2023-05-12"),
                                              ("7", "9753672337", "Vigo", "4238", "21874301P", "2023-05-12"),
                                              ("8", "0000237147", "Huelva", "1736", "21874301P", "2023-05-12"),
                                              ("9", "2738568292", "Burgos", "7926", "21874301P", "2023-05-12"),
                                              ("10", "1897300000", "Toledo", "7735", "21874301P", "2023-05-12"),
                                              ("11", "1966732576", "Badajoz", "0027", "21874301P", "2023-05-12"),
                                              ("12", "1974561473", "Gijon", "8473", "21874301P", "2023-05-12")
                                              )
                                           )
                                          .toDF("id", "clientCode", "location", "storeId", "clientIdCard", "dataDatePart")

//Guardamos dataframe en csv
df.write.csv()

///dbfs/FileStore/tables/filesManagementTask.csv

// COMMAND ----------

df
   .repartition(1)
   .write.format("com.databricks.spark.csv")
   .option("header", "true")
   .save("/FileStore/tables/FManagement")

// COMMAND ----------

dbutils.fs.ls("FileStore/tables/FManagement/part-00000-tid-4923768510658068547-df658a5f-8e29-4121-9d9b-fcca4f2c0259-105-1-c000.csv")

// COMMAND ----------

dbutils.fs.mv("FileStore/tables/FManagement/part-00000-tid-4923768510658068547-df658a5f-8e29-4121-9d9b-fcca4f2c0259-105-1-c000.csv"
,"FileStore/tables/FManagement/miDF.csv")