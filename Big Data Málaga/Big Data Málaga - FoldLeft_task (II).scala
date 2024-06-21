// Databricks notebook source
import org.apache.spark.sql.functions._   
import org.apache.spark.sql.DataFrame

val nullValue = null

  val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "UK", nullValue, "GBP", "2023-05-30"),
                                                    ("2", "ESP", "Euro", "EUR", "2023-05-30"),
                                                    ("3", "HK", nullValue, nullValue, "2023-05-30"),
                                                    ("4", "USA", "Dollar", nullValue, "2023-05-30"),
                                                    ("5", "USA", "Dollar", "USD", "2023-05-30"),
                                                    ("6", "ESP", nullValue, "EUR", "2023-05-30"),
                                                    ("7", "UK", "Libra Esterlina", "GBP", "2023-05-30"),
                                                    ("8", "HK", nullValue, "HKD", "2023-05-30"),
                                                    ("9", "USA", nullValue, "USD", "2023-05-30"),
                                                    ("10", "ESP", nullValue, nullValue, "2023-05-30")
                                                    )
                                                  )
                                                  .toDF("id", "geo", "currName", "currCode", "dataDatePart")



  
  val expected: DataFrame = spark.createDataFrame(Seq(
                                                      ("1", "UK", "GBP", "2023-05-30"),
                                                      ("2", "ESP", "Euro", "2023-05-30"),
                                                      ("3", "HK", nullValue, "2023-05-30"),
                                                      ("4", "USA", "Dollar", "2023-05-30"),
                                                      ("5", "USA", "Dollar", "2023-05-30"),
                                                      ("6", "ESP", "EUR", "2023-05-30"),
                                                      ("7", "UK", "Libra Esterlina", "2023-05-30"),
                                                      ("8", "HK", "HKD", "2023-05-30"),
                                                      ("9", "USA", "USD", "2023-05-30"),
                                                      ("10", "ESP", nullValue, "2023-05-30")
                                                      )
                                                    )
                                                    .toDF("id", "geo", "currency", "dataDatePart")

// COMMAND ----------

inputDf.show()

// COMMAND ----------

expected.show()

// COMMAND ----------


//Las filas las pasamos a listas
val filasListas = inputDf.collect().toList


//Inicializamos la columna currency a null
val currencyDF : DataFrame = inputDf.withColumn("currency", lit (null: String)) //le indicamos el tipo


val respuestaFoldleft : DataFrame  = filasListas.foldLeft(currencyDF) {
    (dfResp, fila) => {//acumulador es df, y el valor siguinete es una fila conforme se itera
          val currName = Option( fila.getAs[String]("currName")) //pos asociativa

          val currCode = Option( fila.getAs[String]("currCode"))

          dfResp.withColumn("currency", when ( 
                                              col("currName").isNotNull, col("currName")
                                                ) 
                                        .otherwise(
                                          col("currCode")
                                        )
                            )
    }   
}

val respuesta : DataFrame= respuestaFoldleft.select("id", "geo", "currency", "dataDatePart")

respuesta.show()
assert(respuesta.collect() sameElements expected.collect())


// COMMAND ----------

