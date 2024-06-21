// Databricks notebook source
// **(1 punto)** Ejercicio 1

// * Leer por separado cada uno de ellos (sin cachear), tratando de que Spark infiera el tipo de dato de cada columna, y **unirlos en un solo DF** que tampoco debe ser cacheada todavía, ya que en el siguiente paso aún realizaremos otro pre-procesamiento.
// * Los cuatro contienen las mismas columnas por lo que no habrá problemas para utilizar la operación `union` encadenada tres veces para crear el DF final.


val arrayRutas = Array("/FileStore/tables/Bike_Share_Toronto_Ridership_Q1_2018.csv", 
                  "/FileStore/tables/Bike_Share_Toronto_Ridership_Q2_2018.csv",
                  "/FileStore/tables/Bike_Share_Toronto_Ridership_Q3_2018.csv",
                  "/FileStore/tables/Bike_Share_Toronto_Ridership_Q4_2018.csv")


// tripsQ1 = None # Primer CSV
// tripsQ2 = None # Segundo CSV
// tripsQ3 = None # Tercer CSV
// tripsQ4 = None # Cuarto CSV
// tripsTorontoRawDF = None # Unión de todos


// COMMAND ----------

val df =  spark.read.format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .option("delimiter",",")
                            .load("/FileStore/tables/Bike_Share_Toronto_Ridership_Q1_2018.csv")
         

// COMMAND ----------

df.printSchema

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._



def cargaDFBicicletas (fichero: String): DataFrame = {


            val df =  spark.read.format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .option("delimiter",",")
                            .load(fichero)


            df

}

// COMMAND ----------

val tripsQ1 = cargaDFBicicletas("/FileStore/tables/Bike_Share_Toronto_Ridership_Q1_2018.csv")
val tripsQ2 = cargaDFBicicletas("/FileStore/tables/Bike_Share_Toronto_Ridership_Q2_2018.csv")
val tripsQ3 = cargaDFBicicletas("/FileStore/tables/Bike_Share_Toronto_Ridership_Q3_2018.csv")
val tripsQ4 = cargaDFBicicletas("/FileStore/tables/Bike_Share_Toronto_Ridership_Q4_2018.csv")

// COMMAND ----------

val tripsTorontoRawDF = tripsQ1.union(tripsQ2)
                                .union(tripsQ3)
                                .union(tripsQ4)
                              

// COMMAND ----------


assert(tripsTorontoRawDF.count == 1922955)


// COMMAND ----------

tripsTorontoRawDF.show()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{unix_timestamp, col}

// **(1 punto)** Ejercicio 2

// * Las columnas `trip_start_time` y `trip_stop_time` son en realidad instantes de tiempo que Spark debería procesar como timestamp. Reemplaza **ambas columnas** por su versión convertida a timestamp, utilizando `withColumn` y donde el nuevo valor de la columna viene dado por el siguiente código:
//         F.from_unixtime(F.unix_timestamp('nombreColumna', 'MM/dd/yyyy HH:mm')).cast("timestamp"))
// El DF resultante debe ser almacenado en la variable `tripsTorontoDF`.

// tripsTorontoDF = None

//                          .withColumn("end_time", to_timestamp($"end_time", "yyyy-MM-dd HH:mm:ss"))
//"MM/dd/yyyy HH:mm"
val tripsTorontoDF = tripsTorontoRawDF.withColumn("trip_start_time",to_timestamp(
                                                                                  col("trip_start_time"), "M/d/yyyy H:mm"
                                                                                )
                                                  )
                                      .withColumn("trip_stop_time",to_timestamp(
                                                                                  col("trip_stop_time"), "M/d/yyyy H:mm"
                                                                                )
                                                  )
                                    


tripsTorontoDF.show()

// COMMAND ----------

val typesDict = tripsTorontoDF.dtypes

//me devuelve tuplas
assert(typesDict(3)._2 == "TimestampType")  //trip_start_time
assert(typesDict(5)._2 == "TimestampType")  //trip_stop_time

// COMMAND ----------

// **(1 punto)** Ejercicio 3

// Partiendo de `tripsTorontoDF`, realizar las siguientes transformaciones encadenadas en este orden para crear un nuevo DF:
// * Primero, debemos quedarnos solamente con las filas donde `trip_start_time` no sea null.
// * Sobre el DF resultado de lo anterior, añadir una columna adicional **Mes** y con el mes representado en **trip_start_time**. Dicha columna será de tipo entero y se puede obtener usando `withColumn` con la función `F.month("colName")`, que recibe un nombre de columna y devuelve un objeto columna de enteros que van de 1 a 12. 
// * Encadenar esta transformación con otra en la que la columna **Mes** sea reemplazada por su traducción a  cadena de caracteres de 3 letras, siendo la correspondencia 1: Ene, 2: Feb, 3: Mar, 4: Abr, 5: May, 6: Jun, 7: Jul, 8: Ago, 9: Sep, 10: Oct, 11: Nov, 12: Dic.
// * Finalmente, añadir una nueva columna **Hora** que contenga la hora de inicio del viaje, aplicando `withColumn` con la función `F.hour("colName")` que recibe un nombre de columna y recibe un objeto columna de enteros de 0 a 23.
// * El DF resultante de todas estas transformaciones debe guardarse en la variable `tripsTorontoTimesDF`, que por tanto tendrá 2 columnas más que el DF original `tripsTorontoDF`, y que debe quedar **cacheado**.

val  tripsTorontoTimesDF = tripsTorontoDF.filter(col("trip_start_time").isNotNull)
                                          .withColumn("Mes", month(col("trip_start_time")))
                                          .withColumn("Mes" , when (col("mes") === 1, lit("Ene"))
                                                              .when (col("mes") === 2, lit("Feb"))
                                                              .when (col("mes") === 3, lit("Mar"))
                                                              .when (col("mes") === 4, lit("Abr"))
                                                              .when (col("mes") === 5, lit("May"))
                                                              .when (col("mes") === 6, lit("Jun"))
                                                              .when (col("mes") === 7, lit("Jul"))
                                                              .when (col("mes") === 8, lit("Ago"))
                                                              .when (col("mes") === 9, lit("Sep"))
                                                              when (col("mes") === 10, lit("Oct"))
                                                              when (col("mes") === 11, lit("Nov"))
                                                              when (col("mes") === 12, lit("Dic"))
                                                      )
                                          .withColumn("Hora", hour(col("trip_start_time")))
                                          .cache()
// df.filter(df.col_X.isNotNull())

tripsTorontoTimesDF.show()

// COMMAND ----------

val tripsPerMonth = tripsTorontoTimesDF.groupBy("Mes").count().sort("Mes").collect()


// COMMAND ----------

assert((tripsPerMonth(0))(1).toString.toInt == 94783)
assert((tripsPerMonth(1))(1).toString.toInt == 281219)
assert((tripsPerMonth(2))(1).toString.toInt == 83324)
assert((tripsPerMonth(3))(1).toString.toInt == 43859)
assert((tripsPerMonth(4))(1).toString.toInt == 49731)
assert((tripsPerMonth(5))(1).toString.toInt == 286316)
assert((tripsPerMonth(6))(1).toString.toInt == 250837)
assert(((tripsPerMonth(7))(1).toString.toInt == 84959) || ((tripsPerMonth(7))(1).toString.toInt == 84969))
assert((tripsPerMonth(8))(1).toString.toInt == 212750)
assert((tripsPerMonth(9))(1).toString.toInt == 104287)
assert((tripsPerMonth(10))(1).toString.toInt == 175879)
assert((tripsPerMonth(11))(1).toString.toInt == 255001)

// COMMAND ----------

// * Partiendo de `tripsTorontoTimesDF`, crear un nuevo DataFrame con **tantas filas como horas tiene el día, y tantas columnas como meses del año** de manera que cada celda indique el **número de viajes** que comenzaron a esa hora en ese mes del año. Guardar el resultado en la variable `tripsPerMonthAndHourDF`, cuyas filas deben quedar ordenadas en base a la hora (de 0 a 23), y cuyas columnas deben estar también ordenadas desde `"Ene"` a `"Dic"`, con `"Hora"` como primera columna.


val tripsPerMonthAndHourDF = tripsTorontoTimesDF.groupBy(col("hora"))
                                                .pivot(col("Mes"))
                                                .count()
                                                .orderBy(col("hora")) 
                                                .select("hora","Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic")
tripsPerMonthAndHourDF.show()

// COMMAND ----------

assert(tripsPerMonthAndHourDF.columns.length == 13)
assert(tripsPerMonthAndHourDF.columns(0) == "hora")
assert(tripsPerMonthAndHourDF.columns(12) == "Dic")
assert(tripsPerMonthAndHourDF.count() == 24)
val todasHoras = tripsPerMonthAndHourDF.collect()
assert(((todasHoras(0))(0) == 0) && ((todasHoras(0))(12)==782))
assert(((todasHoras(23))(0) == 23 && (todasHoras(23))(12) == 1208))

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// **(3 puntos)** Ejercicio 5. 

// Partiendo de `tripsTorontoTimesDF` definido anteriormente, añadir las siguientes columnas:

// * Primero, tres columnas adicionales llamadas `dur_media`, `dur_min`, `dur_max` que contengan, respectivamente, **la duración media, mínima y máxima de los viajes que parten de esa misma estación de origen (from_station_id) a esa misma hora y en ese mismo mes del año**. Es decir, queremos una columna extra para que podamos tener, junto a cada viaje, información agregada de los viajes similares, entendidos como aquellos que salieron a la misma hora de la misma estación. **No se debe utilizar JOIN sino solo funciones de ventana**.


// * A continuación, otra columna adicional `diff_dur_porc` que contenga la diferencia, medida en porcentaje, entre la duración del viaje y la duración media de los viajes similares calculada en el apartado anterior. Dicha diferencia debe calcularse como la resta de la duración del viaje menos la duración media, dividida entre la duración media y multiplicada por 100. El resultado debe obtenerse aplicando operaciones aritméticas con columnas existentes, **sin utilizar `when`**.
// * El DF resultante con las 4 columnas nuevas que hemos añadido debe almacenarse en la variable `tripsTorontoExtraInfoDF`.


val stationWindow = Window.partitionBy("from_station_id", "Mes", "Hora")
val tripsTorontoExtraInfoDF = tripsTorontoTimesDF.withColumn("dur_media", avg("trip_duration_seconds").over(stationWindow))
                                              .withColumn("dur_min", min("trip_duration_seconds").over(stationWindow))
                                              .withColumn("dur_max", max("trip_duration_seconds").over(stationWindow))
                                              .withColumn("diff_dur_porc", ((col("trip_duration_seconds") - col("dur_media")) / col("dur_media")) * 100 )



tripsTorontoExtraInfoDF.show(500, 500, vertical = true)

// COMMAND ----------

val r = tripsTorontoExtraInfoDF.where("trip_id = '2970611'").head()
assert(r.getAs[Double]("dur_media") - 783.366666667 < 0.001)
assert(r.getAs[Double]("diff_dur_porc") - 44.24918088591975 < 0.001)
assert(r.getAs[Integer]("dur_min")  == 167)
assert(r.getAs[Integer]("dur_max") == 2333)


// COMMAND ----------

tripsTorontoExtraInfoDF.printSchema