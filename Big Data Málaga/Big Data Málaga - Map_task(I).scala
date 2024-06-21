// Databricks notebook source
import org.apache.spark.sql.DataFrame

// Input data
val inputData: DataFrame = spark.createDataFrame(
                                                  Seq(
                                                      ("1", "Juan", "Accounting", Map("city" -> "Málaga"), Map("country" -> "España"), "2023-09-19"),
                                                      ("2", "Pablo", "Data", Map("city" -> "Santander"), Map("country" -> "Francia"), "2023-09-19"),
                                                      ("3", "Tomas", "Human Resources", Map("city" -> "Madrid"), Map("country" -> "Inglaterra"), "2023-09-19"),
                                                      ("4", "Roberto", "IT", Map("city" -> "Barcelona"), Map("country" -> "España"), "2023-09-19"),
                                                      ("5", "Miguel", "M&A", Map("city" -> "Bilbao"), Map("country" -> "Venezuela"), "2023-09-19")
                                                      )
                                                  )
                                                  .toDF("id", "name", "department", "citiesMap", "countriesMap", "dataDatePart")
// Expected result
val expected: DataFrame = spark.createDataFrame(
                                                Seq(
                                                    ("1", "Juan", "Accounting", "Málaga", "España", "2023-09-19"),
                                                    ("2", "Pablo", "Data", "Santander", "Francia", "2023-09-19"),
                                                    ("3", "Tomas", "Human Resources", "Madrid", "Inglaterra", "2023-09-19"),
                                                    ("4", "Roberto", "IT", "Barcelona", "España", "2023-09-19"),
                                                    ("5", "Miguel", "M&A", "Bilbao", "Venezuela", "2023-09-19")
                                                    )
                                                )
                                                  .toDF("id", "name", "department", "city", "country", "dataDatePart")

// COMMAND ----------

inputData.show()

// COMMAND ----------

//mapas: clave-> valor ---> accedemos al valor a partir de la clave?

val respuesta = inputData.withColumn("city", col("citiesMap")("city")) //con $ como si se llamara a variable   
                          .withColumn("country", col("countriesMap")("country"))
                          .select("id", "name", "department", "city", "country", "dataDatePart")


// COMMAND ----------

respuesta.show()

// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())
