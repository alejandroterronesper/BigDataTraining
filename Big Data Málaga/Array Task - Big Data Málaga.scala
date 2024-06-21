// Databricks notebook source
// Ejercicio: array_contains con store_id contiene el valor 11

import org.apache.spark.sql.functions._   
import org.apache.spark.sql.DataFrame


final val trueFlag: Boolean = true
final val falseFlag: Boolean = false

val campos = Array ("id", "employeeCode", "location", "storeId", "dataDatePart")

// Input data
val inputDf : DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08"), "2024-05-05")))
.toDF(campos:_*)


// Expected result
val expected: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13"), falseFlag, "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07"), falseFlag, "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05"), falseFlag, "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08"), trueFlag, "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08"), trueFlag, "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



val respuesta : DataFrame = inputDf.withColumn (
                                    "validation",
                                    when (array_contains(col("storeId"), "11"), true)
                                    .otherwise(false)
                                     )
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")
                            

// expected.show()
// respuesta.show()


// COMMAND ----------

assert(expected.collect() sameElements respuesta.collect())


// COMMAND ----------

//Ejercicio: array_distinct sobre la columna storeId

// Input data
val inputDf = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "03"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "05"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "11"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "08"), "2024-05-05")))
.toDF(campos:_*)

// Expected result
val expected02: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "03"), Array("03", "13"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("02", "07", "08"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "05"), Array("22", "05"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "11"), Array("11", "08"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "08"), Array("11", "08"), "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")

val respuesta02: DataFrame = inputDf.withColumn (
                                    "validation",
                                    array_distinct(col("storeId"))
                                     )
                        .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")




// COMMAND ----------

assert(expected02.collect() sameElements respuesta02.collect())


// COMMAND ----------

// Ejercicio: array_except sobre columnas storeId y warehouseId

//ARRAY_EXCEPT: devuelve un array con los elementos que se encuentran
// SOLAMENTE en la columna 1 y no en la columna 2 (parece un left join)

val campos = Array ("id", "employeeCode", "location", "storeId", "warehouseId", "dataDatePart")

// Input data
val inputDf = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"),
"2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), "2024-05-05")))
.toDF(campos:_*)

val respuesta03: DataFrame = inputDf.withColumn("validation", 
                                  array_except(
                                                col("storeId"),
                                                col("warehouseId")  
                                              )
                                  ) 
                                  .select("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")

val expected03: DataFrame = spark.createDataFrame(
                                                  Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), Array("13"),"2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), Array("02","07"), "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), Array("05"),"2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), Array("11", "08","13"), "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), Array.empty[String], "2024-05-05")))
                                    .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")





// COMMAND ----------

assert(expected03.collect() sameElements respuesta03.collect())


// COMMAND ----------

//Ejercicio: array_intersect sobre columnas storeId y warehouseId

//ARRAY_INTERSECT: devuelve un array de la unión de dos arrays

// Input data
val inputDf : DataFrame = spark.createDataFrame(Seq(
                                        ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), "2024-05-05"),
                                        ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("02", "08", "11"), "2024-05-05"),  
                                        ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), "2024-05-05"),
                                        ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), "2024-05-05"),
                                        ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), "2024-05-05")))
                                    .toDF(campos:_*)

val respuesta04 : DataFrame =  inputDf.withColumn("validation",
                                     array_intersect(col("storeId"), col("warehouseId"))
                                    )
                         .select("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")

respuesta.show()

val expected04  : DataFrame = spark.createDataFrame(Seq(
                                          ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), Array("03","21"), "2024-05-05"),
                                          ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("02", "08", "11"), Array("02","08"), "2024-05-05"),
                                          ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), Array("22","10"), "2024-05-05"),
                                          ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"),Array.empty[String], "2024-05-05"),
                                          ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), Array("11", "08","03"), "2024-05-05")))
                                          .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")





// COMMAND ----------

assert(expected04.collect() sameElements respuesta04.collect())


// COMMAND ----------

//Ejercicio: array_join sobre columna storeId con delimitador |

//Concatena los elementos de un array

// Parámetros: 

//   array: un array de cualquier tipo, pero sus elementos se castean a cadena

//   delimiter: cadena que se usa para concatenar los elementos del array

//   nullReplacement: cadena para sustituir los valores NULL del array.


// Input data
val inputDf : DataFrame = spark.createDataFrame(Seq(
                                        ("1", "6245607513", "Málaga", Array("03", "13", "21"), "2024-05-05"),
                                        ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                        ("3", "0987252925", "Valencia", Array("22", "05", "10"), "2024-05-05"),
                                        ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                        ("5", "7239657623", "Bilbao", Array("11", "08", "03"), "2024-05-05")
                                        )
                                    )
                                      .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")



val respuesta05 : DataFrame  = inputDf.withColumn("validation",
                                    array_join(col("storeId"), "|")
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")




val expected05  : DataFrame  = spark.createDataFrame(Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "13", "21"), "03|13|21", "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07", "08"), "02|07|08", "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "10"), "22|05|10", "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "11|08|13", "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08", "03"), "11|08|03", "2024-05-05")
                                                      )
                                                    )
                                                    .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



assert(expected05.collect() sameElements respuesta05.collect())



// COMMAND ----------

assert(expected05.collect() sameElements respuesta05.collect())


// COMMAND ----------

//Ejercicio: array_max sobre columna storeId

// Input data
val inputDf : DataFrame = spark.createDataFrame(Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "13", "21"), "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "10"), "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08", "03"), "2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")

// Expected result
val expected06 : DataFrame = spark.createDataFrame(Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "13", "21"), "21", "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07", "08"), "08", "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "10"), "22", "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "13", "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08", "03"), "11", "2024-05-05")))
                                                    .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



val respuesta06 : DataFrame = inputDf.withColumn("validation", 
                                    array_max( col("storeId")   )
                                    )
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


assert(expected06.collect() sameElements respuesta06.collect())



// COMMAND ----------

assert(expected06.collect() sameElements respuesta06.collect())


// COMMAND ----------

//Ejercicio: array_min sobre columna storeId

val inputDf: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "21"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "10"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "03"), "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "dataDatePart")


val respuesta07: DataFrame = inputDf.withColumn("validation",
                                    array_min(
                                                col("storeId")
                                    ))
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")
    

val expected07: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "21"), "03", "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), "02", "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "10"), "05", "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), "08", "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "03"), "03", "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



assert(expected07.collect() sameElements respuesta07.collect())

// COMMAND ----------

assert(expected07.collect() sameElements respuesta07.collect())

// COMMAND ----------

//Ejercicio: array_position sobre la columna storeId con valor 11

// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05", "11"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "11", "03"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")

// Expected result
val expected08: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "11", "21"), 2, "2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07", "08"), 0, "2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05", "11"), 3, "2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("11", "08", "13"), 1, "2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("08", "11", "03"), 2, "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


val respuesta08: DataFrame = inputDf.withColumn("validation", 
                                                array_position(
                                                  col("storeId"), "11"
                                                )              
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


assert(expected08.collect() sameElements respuesta08.collect())


// COMMAND ----------

assert(expected08.collect() sameElements respuesta08.collect())


// COMMAND ----------

//Ejercico array_remove sobre la columna storeId con valor 11

// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "11"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("08", "11", "03"), "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "dataDatePart")



// Expected result
val expected09: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "11", "21"), Array("03", "21"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("02", "07", "08"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "11"), Array("22", "05"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("08", "13"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("08", "11", "03"), Array("08", "03"), "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



val resultado09: DataFrame = inputDf.withColumn("validation", 
                                                array_remove(
                                                              col("storeId"), "11"
                                                )
                                                )
                                   .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")

                                                
assert(expected09.collect() sameElements resultado09.collect())



// COMMAND ----------

assert(expected09.collect() sameElements resultado09.collect())


// COMMAND ----------

//Ejercicio: array_repeat sobre columna storeId con count 2

//Devuelve un array con el elemento indicado una N cantidad de veces

// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "13"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("10", "05"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("11", "08"), "2024-05-05"))
                                              )
                                              .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")

// Expected result
val expected10: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "13"), Array(Array("03", "13"),Array("03", "13")),"2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07"), Array(Array("02", "07"),Array("02", "07")),"2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05"), Array(Array("22", "05"),Array("22", "05")),"2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("10", "05"), Array(Array("10", "05"),Array("10", "05")),"2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("11", "08"), Array(Array("11", "08"),Array("11", "08")),"2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


val resultado10: DataFrame = inputDf.withColumn("validation",
                                    array_repeat(
                                                  col("storeId"), 2
                                    )
                                              )
                                              .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart") 

assert(expected10.collect() sameElements resultado10.collect())



// COMMAND ----------

assert(expected10.collect() sameElements resultado10.collect())


// COMMAND ----------

//Ejercicio: array_sort sobre columna storeId

//Devuelve un array ordenado

// Si no se introduce el parametro función, lo ordena ascendente

// Si se le pasa una funcion lambda con dos argumentos, representan dos argumentos del array

// DEVUELVE -1,0,1 si es MENOR,IGUAL O MAYOR que el segundo elemento.

// Si la función lambda devuelve NULL, el array sort lanza excepción

// Los null se colocan al final del array .


// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05", "11"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "11", "03"), "2024-05-05"))
                                              )
                                              .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")


// Expected result
val expected11: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "11", "21"), Array("03", "11", "21"), "2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("02", "07", "08"), "2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05", "11"), Array("05", "11", "22"),
                                                    "2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("08", "11", "13"), "2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("08", "11", "03"), Array("03", "08", "11"), "2024-05-05"))
                                                )
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


val resultado11: DataFrame = inputDf.withColumn(
                                    "validation",
                                      array_sort(
                                                      col("storeId")
                                                    )
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



assert(expected11.collect() sameElements resultado11.collect())




// COMMAND ----------

assert(expected11.collect() sameElements resultado11.collect())


// COMMAND ----------

//Ejercicio: sort_array sobre columna storeId y orden descendente
//Tiene dos parámetros:

      //  - El primero es el array
      //  - El segundo es un bool que indica asc o desc, defecto TRUE

// test("should apply array sort_array transformation"){
// Input data
val inputDf: DataFrame = spark.createDataFrame(
                                                Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "11"), "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("08", "11", "03"), "2024-05-05")
                                                    )
                                              )
                          .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")
// Expected result
val expected12: DataFrame = spark.createDataFrame(Seq(
                                                ("1", "6245607513", "Málaga", Array("03", "11", "21"), Array("21", "11", "03"), "2024-05-05"),
                                                ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("08", "07", "02"), "2024-05-05"),
                                                ("3", "0987252925", "Valencia", Array("22", "05", "11"), Array("22", "11", "05"),"2024-05-05"),
                                                ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("13", "11", "08"), "2024-05-05"),
                                                ("5", "7239657623", "Bilbao", Array("08", "11", "03"), Array("11", "08", "03"), "2024-05-05"))
                                                )
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


val resultado12: DataFrame = inputDf.withColumn(
                                    "validation",
                                      sort_array(
                                                  col("storeId")
                                                  ,falseFlag
                                                )
                                    )
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")

assert(expected12.collect() sameElements resultado12.collect())




// COMMAND ----------

assert(expected12.collect() sameElements resultado12.collect())


// COMMAND ----------

//Ejercicio: array_union sobre columna storeId y warehouseId

//Returns an array of the elements in the union of array1 and array2 without duplicates.


val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"),"2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "dataDatePart")


// Expected result
val expected13: DataFrame = spark.createDataFrame(Seq(
("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), Array("03",
"13", "21", "14"), "2024-05-05"),
("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), Array("02",
"07", "08", "03", "11"), "2024-05-05"),
("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), Array("22",
"05", "10", "13"), "2024-05-05"),
("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), Array("11", "08",
"13", "01", "05", "07"), "2024-05-05"),
("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), Array("11", "08",
"03"), "2024-05-05")))
.toDF("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")



val resultado13: DataFrame = inputDf.withColumn(
                                    "validation",
                                      array_union(
                                                  col("storeId"),
                                                  col("warehouseId")
                                                  )
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")
  
assert(expected13.collect() sameElements resultado13.collect())



// COMMAND ----------

assert(expected13.collect() sameElements resultado13.collect())


// COMMAND ----------

//Ejercicio: arrays_overlap sobre columna storeId y warehouseId
//TRUE SI HAY ELEMENTO COMUN
//FALSE SI NO HAY Y NO HAY NULOS
//NULL SI EN ALGUNO DE LOS DOS HAY AL MENOS NULL

// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), "2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), "2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), "2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), "2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "dataDatePart")

// Expected result
val expected14: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "13", "21"), Array("03", "14", "21"), trueFlag,"2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07", "08"), Array("03", "08", "11"), trueFlag,"2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05", "10"), Array("10", "13", "22"), trueFlag,"2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("01", "05", "07"), falseFlag,"2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("11", "08", "03"), Array("03", "08", "11"), trueFlag,"2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")



val resultado14: DataFrame = inputDf.withColumn("validation",
                                    arrays_overlap(
                                                    col("storeId"),
                                                    col("warehouseId")
                                                  )
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")

assert(expected14.collect() sameElements resultado14.collect())


// COMMAND ----------

assert(expected14.collect() sameElements resultado14.collect())


// COMMAND ----------

//Ejercicio: arrays_zip sobre columnas storeId y warehouseId
import org.apache.spark.sql.types._


//Array zip: mergea dos array como estructura de pares

// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "13"), Array("14", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), Array("03", "08"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05"), Array("10", "13"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08"), Array("01", "05"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "03"), Array("11", "08"), "2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "dataDatePart")


// Expected result
val schema = StructType(
                        Seq(
                              StructField("id", StringType, nullable = trueFlag),
                              StructField("employeeCode", StringType, nullable = trueFlag),
                              StructField("location", StringType, nullable = trueFlag),
                              StructField("storeId", ArrayType(StringType, containsNull = trueFlag), nullable = trueFlag),
                              StructField("warehouseId", ArrayType(StringType, containsNull = trueFlag), nullable = trueFlag),
                              StructField("validation", ArrayType(
                                                                  StructType(
                                                                                  Seq(
                                                                                   StructField("storeId", StringType, nullable = trueFlag),
                                                                                    StructField("warehouseId", StringType, nullable = trueFlag)
                                                                                    )
                                                                              )
                                                                  ), nullable = trueFlag),
                              StructField("dataDatePart", StringType, nullable = trueFlag)
                            )
                        )

val expectedData: Seq[Row] = Seq(
                                  Row("1", "6245607513", "Málaga", Array("03", "13"), Array("14", "21"), Array(Row("03", "14"), Row("13", "21")), "2024-05-05"),
                                  Row("2", "1283973874", "Málaga", Array("02", "07"), Array("03", "08"), Array(Row("02","03"), Row("07", "08")), "2024-05-05"),
                                  Row("3", "0987252925", "Valencia", Array("22", "05"), Array("10", "13"), Array(Row("22","10"), Row("05", "13")), "2024-05-05"),
                                  Row("4", "2937638976", "Bilbao", Array("11", "08"), Array("01", "05"), Array(Row("11", "01"), Row("08", "05")), "2024-05-05"),
                                  Row("5", "7239657623", "Bilbao", Array("08", "03"), Array("11", "08"), Array(Row("08", "11"), Row("03", "08")), "2024-05-05")
                                )


val expected15: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), schema)


val resultado15: DataFrame = inputDf.withColumn(
                                    "validation",
                                    arrays_zip(
                                                col("storeId"),
                                                col("warehouseId")
                                              )
                                  )
                        .select("id", "employeeCode","location", "storeId", "warehouseId", "validation","dataDatePart")


assert(expected15.collect() sameElements resultado15.collect())


// COMMAND ----------

assert(expected15.collect() sameElements resultado15.collect())


// COMMAND ----------

//Ejercicio: map_from_arrays sobre columnas storeId y warehouseId

//crea un mapa clave valor con los valores de un array
// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "13"), Array("14", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), Array("03", "08"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05"), Array("10", "13"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08"), Array("01", "05"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "03"), Array("11", "08"), "2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "dataDatePart")
// Expected result
val expected16: DataFrame = spark.createDataFrame(Seq( 
                                                      ("1", "6245607513", "Málaga", Array("03", "13"), Array("14", "21"), Map("03" -> "14", "13" ->"21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), Array("03", "08"), Map("02" -> "03", "07" ->"08"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05"), Array("10", "13"), Map("22" -> "10", "05"-> "13"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08"), Array("01", "05"), Map("11" -> "01", "08" ->"05"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "03"), Array("11", "08"), Map("08" -> "11", "03" ->"08"), "2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")



val resultado16: DataFrame = inputDf.withColumn(
                                    "validation",
                                    map_from_arrays(
                                                     col( "storeId"),
                                                     col( "warehouseId")
                                                    )
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "warehouseId", "validation", "dataDatePart")

assert(expected16.collect() sameElements resultado16.collect())



// COMMAND ----------

assert(expected16.collect() sameElements resultado16.collect())


// COMMAND ----------

// Ejercicio: shuffle sobre columna storrId
// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                ("2", "1283973874", "Málaga", Array("02", "07", "08"), "2024-05-05"),
                                                ("3", "0987252925", "Valencia", Array("22", "05", "11"), "2024-05-05"),
                                                ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                ("5", "7239657623", "Bilbao", Array("08", "11", "03"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")
// Expected result cannot be calculated, because shuffle transformation give a random
// number to the array order


val arrayOrden: DataFrame = inputDf.withColumn(
                                    "validation",
                                    shuffle(col("storeId"))
                                    )


arrayOrden.show()                                    

// COMMAND ----------

// Ejercicio: size sobre columna storeId
// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05", "11", "19"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("08", "11"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")


// Expected result
val expected17: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "11", "21"), 3, "2024-05-05"),
                                                    ("2", "1283973874", "Málaga", Array("02", "07"), 2, "2024-05-05"),
                                                    ("3", "0987252925", "Valencia", Array("22", "05", "11", "19"), 4, "2024-05-05"),
                                                    ("4", "2937638976", "Bilbao", Array("11", "08", "13"), 3, "2024-05-05"),
                                                    ("5", "7239657623", "Bilbao", Array("08", "11"), 2, "2024-05-05")))
                                                  .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")




val resultado17: DataFrame = inputDf.withColumn(
                                    "validation",
                                    size (col ("storeId"))
                                    )
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


assert(expected17.collect() sameElements resultado17.collect())


// COMMAND ----------

assert(expected17.collect() sameElements resultado17.collect())


// COMMAND ----------

// Ejercicio: slice sobre columna storeId con inicio en posicion 2 y longitud 2
// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                ("1", "6245607513", "Málaga", Array("03", "11", "21"), "2024-05-05"),
                                                ("2", "1283973874", "Málaga", Array("02", "07"), "2024-05-05"),
                                                ("3", "0987252925", "Valencia", Array("22", "05", "11", "19"), "2024-05-05"),
                                                ("4", "2937638976", "Bilbao", Array("11", "08", "13"), "2024-05-05"),
                                                ("5", "7239657623", "Bilbao", Array("08", "11"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")
// Expected result
val expected18: DataFrame = spark.createDataFrame(Seq(
                                                      ("1", "6245607513", "Málaga", Array("03", "11", "21"), Array("11", "21"), "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07"), Array("07"), "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05", "11", "19"), Array("05", "11"),"2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08", "13"), Array("08", "13"), "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("08", "11"), Array("11"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")



val resultado18: DataFrame = inputDf.withColumn(
                                    "validation",
                                    slice(
                                          col("storeId"),
                                          2, //POSICION
                                          2, //LONGITUD

                                    )
                                  )
                                  .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")

assert(expected18.collect() sameElements resultado18.collect())


// COMMAND ----------

assert(expected18.collect() sameElements resultado18.collect())


// COMMAND ----------

// Ejercicio: explode sobre columna storeId



// Input data
val inputDf: DataFrame = spark.createDataFrame(Seq(
                                                  ("1", "6245607513", "Málaga", Array("03", "13"), "2024-05-05"),
                                                  ("2", "1283973874", "Málaga", Array("02", "07"), "2024-05-05"),
                                                  ("3", "0987252925", "Valencia", Array("22", "05"), "2024-05-05"),
                                                  ("4", "2937638976", "Bilbao", Array("11", "08"), "2024-05-05"),
                                                  ("5", "7239657623", "Bilbao", Array("11", "08"), "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "dataDatePart")


// Expected result
 val expected19: DataFrame = spark.createDataFrame(Seq(
                                                    ("1", "6245607513", "Málaga", Array("03", "13"), "03", "2024-05-05"),
                                                      ("1", "6245607513", "Málaga", Array("03", "13"), "13", "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07"), "02", "2024-05-05"),
                                                      ("2", "1283973874", "Málaga", Array("02", "07"), "07", "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05"), "22", "2024-05-05"),
                                                      ("3", "0987252925", "Valencia", Array("22", "05"), "05", "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08"), "11", "2024-05-05"),
                                                      ("4", "2937638976", "Bilbao", Array("11", "08"), "08", "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08"), "11", "2024-05-05"),
                                                      ("5", "7239657623", "Bilbao", Array("11", "08"), "08", "2024-05-05")))
                                                .toDF("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")

val resultado19: DataFrame = inputDf.withColumn(
                                    "validation",
                                    explode(
                                            col("storeId").as("storeId")
                                            
                                            )
                                    )
                                    .select("id", "employeeCode", "location", "storeId", "validation", "dataDatePart")


assert(expected19.collect() sameElements resultado19.collect())


// COMMAND ----------

assert(expected.collect() sameElements respuesta.collect())
assert(expected02.collect() sameElements respuesta02.collect())
assert(expected03.collect() sameElements respuesta03.collect())
assert(expected04.collect() sameElements respuesta04.collect())
assert(expected05.collect() sameElements respuesta05.collect())
assert(expected06.collect() sameElements respuesta06.collect())
assert(expected07.collect() sameElements respuesta07.collect())
assert(expected08.collect() sameElements respuesta08.collect())
assert(expected09.collect() sameElements resultado09.collect())
assert(expected10.collect() sameElements resultado10.collect())
assert(expected11.collect() sameElements resultado11.collect())
assert(expected12.collect() sameElements resultado12.collect())
assert(expected13.collect() sameElements resultado13.collect())
assert(expected14.collect() sameElements resultado14.collect())
assert(expected15.collect() sameElements resultado15.collect())
assert(expected16.collect() sameElements resultado16.collect())
assert(expected17.collect() sameElements resultado17.collect())
assert(expected18.collect() sameElements resultado18.collect())
assert(expected19.collect() sameElements resultado19.collect())
