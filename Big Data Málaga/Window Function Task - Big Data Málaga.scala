// Databricks notebook source
import org.apache.spark.sql.DataFrame
// Ejercicio 1: debes obtener mediante la aplicacion de row_number, el registro con mayor
// dataDatePart de cada cliente (clientId)

val inputDf: DataFrame = spark.createDataFrame(
                                                Seq(
                                                      ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386","2023-05-03"),
                                                      ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-04-20"),  
                                                      ("3", "4382", "0987252925", "Valencia", "28.3", "EUR", "0872","2023-01-31"),
                                                      ("4", "2847", "2937638976", "Bilbao", "12.97", "EUR", "7747","2023-02-27"),
                                                      ("5", "8446", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-03-16"),
                                                      ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983", "2023-04-22"),
                                                      ("7", "8446", "9753672337", "Vigo", "8.49", "EUR", "4238","2023-05-17"),
                                                      ("8", "8446", "0000237147", "Huelva", "34.66", "EUR", "1736","2023-04-10"),
                                                      ("9", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-02-26"),
                                                      ("10", "8446", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-24"),
                                                      ("11", "4382", "1966732576", "Badajoz", "18.33", "EUR", "0027","2023-04-30"),
                                                      ("12", "2847", "1974561473", "Gijon", "3.57", "EUR", "8473","2023-01-20")
                                                    )
                                                )
                                                  .toDF("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")



val expected: DataFrame = spark.createDataFrame(
                                                Seq(
                                                      ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386","2023-05-03"),
                                                      ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-04-20"),
                                                      ("10", "8446", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-24")
                                                    )
                                                )


// COMMAND ----------

//Se crea ventana
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


// La particion de filas se hace sobre la col clientId,
// y lo ordenamos por datePart, para sacar el mayor
val miVentana = Window.partitionBy("clientId").orderBy(col("dataDatePart").desc)  


//aplicamos método de row number
var respuesta = inputDf.withColumn("particionClientId",
                                                       row_number().over(miVentana)
                                  ) 

// COMMAND ----------

respuesta.show()

// COMMAND ----------

respuesta = respuesta.filter(
                              col("particionClientId") === 1 //sacamos las filas que tengan la particion 1, que seran las mas altas
                            )
                            .orderBy(
                                      col("dataDatePart").asc //ordenamos
                                    )

respuesta = respuesta.orderBy(col ("storeId")) //ordenamos para que saque los ids numerados

// COMMAND ----------

respuesta.show()

// COMMAND ----------

respuesta = respuesta.select(
                              "id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart"
                            )

// COMMAND ----------

expected.show()

// COMMAND ----------

respuesta.show()

// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect()) //es correcto


// COMMAND ----------

// Ejercicio 2: Obtener el sumatorio de los pagos realizafos por cada cliente, mostrando un
// unico registro por cliente y quedandote solo para la slaida con los campo id, clienteId,
// payment, currency


val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386","2023-05-03"),
                                                    ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-04-20"),
                                                    ("3", "8446", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-03-16"),
                                                    ("4", "2847", "2937638976", "Bilbao", "12.97", "EUR", "7747","2023-02-27"),
                                                    ("5", "4382", "0987252925", "Valencia", "28.3", "EUR", "0872","2023-01-31"),
                                                    ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983","2023-04-22"),
                                                    ("7", "8446", "9753672337", "Vigo", "8.49", "EUR", "4238", "2023-05-17"),
                                                    ("8", "8446", "0000237147", "Huelva", "34.67", "EUR", "1736","2023-04-10"),
                                                    ("9", "8446", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-24"),
                                                    ("10", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-02-26")))
                                                    .toDF("id", "clientId", "transactionCode", "location", "payment", "currency","storeId", "dataDatePart")

                                                    
val expected: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "4382", "91.34", "EUR"),
                                                    ("2", "2847", "15.96", "EUR"),
                                                    ("3", "8446", "75.85", "EUR")))
                                                    .toDF("id", "clientId", "payment", "currency")




// COMMAND ----------

inputDf.printSchema

// COMMAND ----------

val ventanaParticion =  Window.partitionBy("clientId") //Hacemos partición sobre idCliente

val resultado : DataFrame = inputDf.withColumn(
                                                "payment", sum(col("payment")).over(ventanaParticion).cast("String")  // hacemos el sumatorio a la columna de pago sobre la particion
                                              )
                                   .withColumn(
                                                "registro", row_number().over(ventanaParticion.orderBy(col("id"))) //aplicamos el row number para sacar el nº de particiones
                                              )
                                   .filter(
                                            col("registro") === 1 // filtramos por la particion
                                          )
                                   .orderBy(
                                            col("id") 
                                            )
                                   .select("id", "clientId", "payment", "currency")
                                              

resultado.show()
expected.show()


// COMMAND ----------

assert(resultado.collect() sameElements expected.collect())


// COMMAND ----------


resultado.printSchema
expected.printSchema

// COMMAND ----------

// Ejercicio 3: Utilizando dense_rank debes obtener los registros por cada cliente que tenga el
// mayor dataDatePart


val inputDf: DataFrame = spark.createDataFrame(
                                                  Seq( 
                                                      ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386","2023-05-03"),
                                                      ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-05-04"),
                                                      ("3", "4382", "0987252925", "Valencia", "28.3", "EUR", "0872","2023-05-01"), 
                                                      ("4", "2847", "2937638976", "Bilbao", "12.97", "EUR", "7747","2023-05-03"),
                                                      ("5", "8446", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-05-02"),
                                                      ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983","2023-05-04"),
                                                      ("7", "8446", "9753672337", "Vigo", "8.49", "EUR", "4238","2023-05-02"),
                                                      ("8", "8446", "0000237147", "Huelva", "34.66", "EUR", "1736","2023-05-01"),
                                                      ("9", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-05-04"),
                                                      ("10", "8446", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-01"),
                                                      ("11", "4382", "1966732576", "Badajoz", "18.33", "EUR", "0027","2023-05-03"),
                                                      ("12", "2847", "1974561473", "Gijon", "3.57", "EUR", "8473","2023-05-03")
                                                      )
                                                )
                                                      .toDF("id", "clientId", "transactionCode", "location", "payment", "currency",
                                                          "storeId", "dataDatePart")




val expected: DataFrame = spark.createDataFrame(Seq
                                                    (
                                                      ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-05-04"),
                                                      ("5", "8446", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-05-02"),
                                                      ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983","2023-05-04"),
                                                      ("7", "8446", "9753672337", "Vigo", "8.49", "EUR", "4238","2023-05-02"),
                                                      ("9", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-05-04")
                                                    )
                                                )
                                                      .toDF("id", "clientId", "transactionCode", "location", "payment", "currency",
                                                            "storeId", "dataDatePart")

// COMMAND ----------

val miVentana = Window.partitionBy("clientId").orderBy(col("dataDatePart").desc) //hacemos particion sobre cliente Id

val respuesta: DataFrame = inputDf.withColumn("particionClientId", dense_rank().over(miVentana)) //sacamos columna con la particion y la funcion dense rank
                                  .filter(col("particionClientId") === 1) //filtramos por columna 1 
                                  .drop("particionClientId") //la borramos y nos quedamos con las filas con mayor particion
                                  .orderBy(col("id"))

respuesta.show()

// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())


// COMMAND ----------

// Ejercicio 4: se debe obtener una nueva columna que identifique la locacizacion del registro
// anterior al que se muestra
val nullValue = null

val inputDf: DataFrame = spark.createDataFrame(
                                                Seq(
                                                      ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386","2023-05-03"),
                                                      ("2", "4382", "1283973874", "Madrid", "2.99", "EUR", "2287","2023-05-04"),
                                                      ("3", "4382", "0987252925", "Valencia", "28.3", "EUR", "0872", "2023-05-01"),
                                                      ("4", "4382", "2937638976", "Bilbao", "12.97", "EUR", "7747","2023-05-03"),
                                                      ("5", "4382", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-05-02"),
                                                      ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983","2023-05-04"),
                                                      ("7", "4382", "9753672337", "Vigo", "8.49", "EUR", "4238","2023-05-02"),
                                                      ("8", "4382", "0000237147", "Huelva", "34.66", "EUR", "1736","2023-05-01"),
                                                      ("9", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-05-04"),
                                                      ("10", "4382", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-01"),
                                                      ("11", "4382", "1966732576", "Badajoz", "18.33", "EUR", "0027","2023-05-03"),
                                                      ("12", "4382", "1974561473", "Gijon", "3.57", "EUR", "8473","2023-05-03")
                                                    )
                                                )
                                              .toDF("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")


val expected: DataFrame = spark.createDataFrame(
                                                Seq(
                                                    ("1", "4382", "6245607513", "Malaga", nullValue, "45.23", "EUR", "0386", "2023-05-03"), 
                                                    ("2", "4382", "1283973874", "Madrid", "Malaga", "2.99", "EUR", "2287", "2023-05-04"),
                                                    ("3", "4382", "0987252925", "Valencia", "Madrid", "28.3", "EUR","0872", "2023-05-01"),
                                                    ("4", "4382", "2937638976", "Bilbao", "Valencia", "12.97", "EUR","7747", "2023-05-03"),
                                                    ("5", "4382", "7239657623", "Barcelona", "Bilbao", "5.28", "EUR","1038", "2023-05-02"),
                                                    ("6", "4382", "2786527574", "A Coruña", "Barcelona", "0.43", "EUR","0983", "2023-05-04"),
                                                    ("7", "4382", "9753672337", "Vigo", "A Coruña", "8.49", "EUR", "4238","2023-05-02"),
                                                    ("8", "4382", "0000237147", "Huelva", "Vigo", "34.66", "EUR", "1736", "2023-05-01"),
                                                    ("9", "4382", "2738568292", "Burgos", "Huelva", "17.38", "EUR","7926", "2023-05-04"),
                                                    ("10", "4382", "1897300000", "Toledo", "Burgos", "27.41", "EUR","7735", "2023-05-01"),
                                                    ("11", "4382", "1966732576", "Badajoz", "Toledo", "18.33", "EUR","0027", "2023-05-03"),
                                                    ("12", "4382", "1974561473", "Gijon", "Badajoz", "3.57", "EUR","8473", "2023-05-03")
                                                    )
                                                  )
                                              .toDF("id", "clientId", "transactionCode", "location", "previousLocation", "payment", "currency", "storeId", "dataDatePart")

//
                                          

// COMMAND ----------

inputDf.show()


// COMMAND ----------

val ventanaVar = Window.partitionBy("clientId").orderBy(col("clientId").desc) //hacemos particion a partir de clientID

var respuesta = inputDf.withColumn(
                                    "previousLocation",
                                     lag( col("location"), 1) //aplicamos funcion lag, 
                                     .over(ventanaVar)                            
                                  )
                                  .select("id", "clientId", "transactionCode", "location", "previousLocation", "payment", "currency", "storeId", "dataDatePart")
    
respuesta.show()

// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())


// COMMAND ----------

// Ejercicio 5: aqui se pide obtener el valor de location de dos registros siguiente, por ej: en el
// registro 1 se debera mostrar el valor de location del registro 3

val inputDf: DataFrame = spark.createDataFrame(
                                                Seq(
                                                    ("1", "4382", "6245607513", "Malaga", "45.23", "EUR", "0386", "2023-05-03"),
                                                    ("2", "4382", "1283973874", "Madrid", "2.99", "EUR", "2287", "2023-05-04"),
                                                    ("3", "4382", "0987252925", "Valencia", "28.3", "EUR", "0872", "2023-05-01"),
                                                    ("4", "4382", "2937638976", "Bilbao", "12.97", "EUR", "7747", "2023-05-03"),
                                                    ("5", "4382", "7239657623", "Barcelona", "5.28", "EUR", "1038","2023-05-02"),
                                                    ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983", "2023-05-04"),
                                                    ("7", "4382", "9753672337", "Vigo", "8.49", "EUR", "4238", "2023-05-02"),
                                                    ("8", "4382", "0000237147", "Huelva", "34.66", "EUR", "1736","2023-05-01"),
                                                    ("9", "4382", "2738568292", "Burgos", "17.38", "EUR", "7926","2023-05-04"),
                                                    ("10", "4382", "1897300000", "Toledo", "27.41", "EUR", "7735","2023-05-01"),
                                                    ("11", "4382", "1966732576", "Badajoz", "18.33", "EUR", "0027","2023-05-03"),
                                                    ("12", "4382", "1974561473", "Gijon", "3.57", "EUR", "8473","2023-05-03")
                                                    )
                                              )
                                            .toDF("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")


val expected: DataFrame = spark.createDataFrame(
                                                Seq(
                                                    ("1", "4382", "6245607513", "Malaga", "Valencia", "45.23", "EUR", "0386", "2023-05-03"),
                                                    ("2", "4382", "1283973874", "Madrid", "Bilbao", "2.99", "EUR", "2287","2023-05-04"),
                                                    ("3", "4382", "0987252925", "Valencia", "Barcelona", "28.3", "EUR","0872", "2023-05-01"),
                                                    ("4", "4382", "2937638976", "Bilbao", "A Coruña", "12.97", "EUR","7747", "2023-05-03"),
                                                    ("5", "4382", "7239657623", "Barcelona", "Vigo", "5.28", "EUR","1038", "2023-05-02"),
                                                    ("6", "4382", "2786527574", "A Coruña", "Huelva", "0.43", "EUR", "0983", "2023-05-04"),
                                                    ("7", "4382", "9753672337", "Vigo", "Burgos", "8.49", "EUR", "4238", "2023-05-02"),
                                                    ("8", "4382", "0000237147", "Huelva", "Toledo", "34.66", "EUR", "1736", "2023-05-01"),
                                                    ("9", "4382", "2738568292", "Burgos", "Badajoz", "17.38", "EUR", "7926", "2023-05-04"),
                                                    ("10", "4382", "1897300000", "Toledo", "Gijon", "27.41", "EUR", "7735", "2023-05-01"),
                                                    ("11", "4382", "1966732576", "Badajoz", nullValue, "18.33", "EUR", "0027", "2023-05-03"),
                                                    ("12", "4382", "1974561473", "Gijon", nullValue, "3.57", "EUR",  "8473", "2023-05-03")
                                                    )
                                                )
                                              .toDF("id", "clientId", "transactionCode", "location", "nextLocation", "payment", "currency", "storeId", "dataDatePart")

// COMMAND ----------

val ventanaVar = Window.partitionBy("clientId").orderBy(col("clientId").desc) //hacemos particion a partir de clientID

var respuesta = inputDf.withColumn(
                                    "nextLocation",
                                     lag( col("location"), -2) //aplicamos funcion lag, 
                                     .over(ventanaVar)                            
                                  )
                                  .select("id", "clientId", "transactionCode", "location", "nextLocation", "payment", "currency", "storeId", "dataDatePart")
expected.show()    
respuesta.show()




// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())

// COMMAND ----------

// Ejercicio 6: en este ultimo se pide obtener el valor minimo de la columna payment por
// cliente, debes obtener un registro por cliente con el valor inferior registrado



val inputDf: DataFrame = spark.createDataFrame(
                                                Seq(
                                                    ("1", "4382", "6245607513", "Malaga", "5.23", "EUR", "0386","2023-05-03"),
                                                    ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287", "2023-05-04"),
                                                    ("3", "4382", "0987252925", "Valencia", "8.38", "EUR", "0872", "2023-05-01"),
                                                    ("4", "2847", "2937638976", "Bilbao", "7.97", "EUR", "7747", "2023-05-03"),
                                                    ("5", "8446", "7239657623", "Barcelona", "1.28", "EUR", "1038", "2023-05-02"),
                                                    ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983", "2023-05-04"),
                                                    ("7", "8446", "9753672337", "Vigo", "8.49", "EUR", "4238", "2023-05-02"),
                                                    ("8", "8446", "0000237147", "Huelva", "4.66", "EUR", "1736", "2023-05-01"),
                                                    ("9", "4382", "2738568292", "Burgos", "5.38", "EUR", "7926", "2023-05-04"),
                                                    ("10", "8446", "1897300000", "Toledo", "7.41", "EUR", "7735", "2023-05-01"),
                                                    ("11", "4382", "1966732576", "Badajoz", "8.33", "EUR", "0027",  "2023-05-03"),
                                                    ("12", "2847", "1974561473", "Gijon", "3.57", "EUR", "8473", "2023-05-03")
                                                    )
                                                )   
                                              .toDF("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")





val expected: DataFrame = spark.createDataFrame( Seq( 
                                                     ("2", "2847", "1283973874", "Madrid", "2.99", "EUR", "2287", "2023-05-04"), 
                                                     ("5", "8446", "7239657623", "Barcelona", "1.28", "EUR", "1038","2023-05-02"),
                                                     ("6", "4382", "2786527574", "A Coruña", "0.43", "EUR", "0983", "2023-05-04")
                                                     )
                                                )
                                                .toDF("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")

// COMMAND ----------

val ventanaVar = Window.partitionBy("clientId").orderBy(col("payment").desc)

val respuesta = inputDf.withColumn("payamentMin", row_number()
                                                              .over(ventanaVar
                                                                              .orderBy(col("payment"))
                                                                    )
                                  )
                                  .filter(col("payamentMin") === 1)
                                  .orderBy(col("id"))
                                  .select("id", "clientId", "transactionCode", "location", "payment", "currency", "storeId", "dataDatePart")




respuesta.show()


// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())
