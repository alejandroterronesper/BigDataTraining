// Databricks notebook source
//Leerlo tratando de que Spark infiera el tipo de dato de cada columna, y cachearlo.

val rutaCSV = "/FileStore/tables/accidents_uk.csv"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
final val trueFlag: Boolean = true


//Creamos schema
val miEschema = StructType(
                        Seq(
                              StructField("Accident_Index", StringType, nullable = trueFlag),
                              StructField("Latitude", DoubleType, nullable = trueFlag),
                              StructField("Longitude", DoubleType, nullable = trueFlag),
                              StructField("Region", StringType, nullable = trueFlag),
                              StructField("Urban_or_Rural_Area", StringType, nullable = trueFlag),
                              StructField("X1st_Road_Class", StringType, nullable = trueFlag),
                              StructField("Driver_IMD_Decile",IntegerType, nullable = trueFlag),
                              StructField("Speed_limit", IntegerType, nullable = trueFlag),
                              StructField("Road_Type", StringType, nullable = trueFlag),
                              StructField("Road_Surface_Conditions", StringType, nullable = trueFlag),
                              StructField("Weather", StringType, nullable = trueFlag),
                              StructField("High_Wind", StringType, nullable = trueFlag),
                              StructField("Lights", StringType, nullable = trueFlag),
                              StructField("Datetime", DateType, nullable = trueFlag),
                              StructField("Year", IntegerType, nullable = trueFlag),
                              StructField("Season", IntegerType, nullable = trueFlag),
                              StructField("Month_of_Year", IntegerType, nullable = trueFlag),
                              StructField("Day_of_Month", IntegerType, nullable = trueFlag),
                              StructField("Day_of_Week", IntegerType, nullable = trueFlag),
                              StructField("Hour_of_Day", DoubleType, nullable = trueFlag),
                              StructField("Number_of_Vehicles", IntegerType, nullable = trueFlag),
                              StructField("Age_of_Driver", IntegerType, nullable = trueFlag),
                              StructField("Age_of_Vehicle", IntegerType, nullable = trueFlag),
                              StructField("Junction_Detail", StringType, nullable = trueFlag),
                              StructField("Junction_Location", StringType, nullable = trueFlag),
                              StructField("X1st_Point_of_Impact", StringType, nullable = trueFlag),
                              StructField("Driver_Journey_Purpose", StringType, nullable = trueFlag),
                              StructField("Engine_CC", IntegerType, nullable = trueFlag),
                              StructField("Propulsion_Code", StringType, nullable = trueFlag),
                              StructField("Vehicle_Make", StringType, nullable = trueFlag),
                              StructField("Vehicle_Category", StringType, nullable = trueFlag),
                              StructField("Vehicle_Manoeuvre", StringType, nullable = trueFlag),
                              StructField("Accident_Severity", StringType, nullable = trueFlag)
                    
                            )
                        )


val accidentesDF = spark.read.format("csv")
                            .option("header", "true")
                            .option("delimiter",",")
                            .schema(miEschema)
                            .load(rutaCSV)
accidentesDF.cache()

// COMMAND ----------

accidentesDF.printSchema

// COMMAND ----------

 assert(accidentesDF.schema(1).dataType == DoubleType)



// accidentesDF.schema(1).dataType

// COMMAND ----------

// Discretizar la variable **Age_of_Vehicle** utilizando un bucketizer (sin crear un pipeline) en los puntos de corte (0, 2, 5, 10, 15, 20, 35). La discretización debe quedar en una nueva columna de tipo Double llamada **Edad_Vehiculo**.
import org.apache.spark.ml.feature.Bucketizer

val varD = Array(Double.NegativeInfinity, 0, 2, 5, 10, 15, 20, 35, Double.PositiveInfinity)


//objeto bucketizar
val miBucket = new Bucketizer ()
    .setInputCol("Age_of_Vehicle")
    .setOutputCol("Edad_Vehiculo")
    .setSplits(varD)


    
val dataDiscretizada = miBucket.transform(accidentesDF)




// COMMAND ----------

// Al hacer lo de las discretizacion con el bucketizer
// sea asignan unos valores a intervalos declarados en el array
// es decir si el valor es 1 coge el valor [0,2) que se encuentra en la pos 1 del array

dataDiscretizada.select(col("Age_of_Vehicle"),col("Edad_Vehiculo")).show()

// COMMAND ----------

//Comrpobación del bucketizer
assert(dataDiscretizada.columns.contains("Edad_Vehiculo"))
assert(dataDiscretizada.schema.fields.map(_.name).contains("Edad_Vehiculo"))


// COMMAND ----------

dataDiscretizada.schema.fields.map(_.name) //map le aplica una funcion a un conjunto de elementos, en este caso a fields que es un array




// COMMAND ----------

dataDiscretizada.select("Age_of_Driver").show()

// COMMAND ----------

// Crear un nuevo DF donde la columna "Age_of_Driver" ha sido reemplazada 
// por otra de tipo string en la que los valores 1 y 2 son "Adolescente", 
// los valores 3 y 4 por "Joven", los valores 5 y 6 por "Adulto", 
// y los demás valores se dejan sin modificar.
// dataDiscretizada.select("Age_of_Driver").show()
val accidentesAgeDF  = dataDiscretizada.withColumn("Age_of_Driver", when( (col("Age_of_Driver") === 1 ) || (col("Age_of_Driver") === 2), lit("Adolescente"))
                                                                   .when( (col("Age_of_Driver") === 3) || (col("Age_of_Driver") === 4), lit("Joven"))
                                                                   .when( (col("Age_of_Driver") === 5) ||  (col("Age_of_Driver") === 6), lit("Adulto"))
                                                                   .otherwise(col("Age_of_Driver"))
                                                    )



accidentesAgeDF.select("Age_of_Driver").show()
accidentesAgeDF.printSchema

// COMMAND ----------

assert(accidentesAgeDF.dtypes(21)._2 == "StringType")
val collectedDF = accidentesAgeDF.groupBy("Age_of_Driver").count().orderBy("count").collect()





// COMMAND ----------

print(collectedDF)


// COMMAND ----------



assert((collectedDF(0).get(1) == 9195) && (collectedDF(0).get(0) == "8"))
assert((collectedDF(1).get(1) == 13338) && (collectedDF(1).get(0) == "7"))
assert((collectedDF(2).get(1) == 57174) && (collectedDF(2).get(0) == "Adolescente"))
assert((collectedDF(3).get(1) == 67138) && (collectedDF(3).get(0) == "Adulto"))
assert((collectedDF(4).get(1) == 104987) && (collectedDF(4).get(0) == "Joven"))


// COMMAND ----------

// Partiendo de `accidentesDF`, crear un nuevo DataFrame de una sola fila que contenga, **por este orden de columnas**, el **número** de categorías existentes para el propósito del viaje, para el tipo de maniobra del vehículo, para las condiciones de la calzada y para la severidad del accidente. Pista: crear las columnas al vuelo con `select`(). Renombrar cada columna de conteo para que se llame igual que la propia columna que estamos contando.
val numeroCategoriasDF = accidentesDF.select(
                                            count(col("Driver_Journey_Purpose")).as("Proposito_viaje"),
                                            count(col("Road_type")).as("Condiciones_calzada"),
                                            count(col("Vehicle_Manoeuvre")).as("Maniobra_vehiculo"),
                                            count(col("Accident_Severity")).as("Severidad_accidente")
                                            
                                            ).distinct


numeroCategoriasDF.show()



// COMMAND ----------



// COMMAND ----------

assert((numeroCategoriasDF.columns).length  == 4)
assert(numeroCategoriasDF.count == 1)

// COMMAND ----------

// Partiendo de `accidentesAgeDF` definido anteriormente, crear un nuevo DataFrame con tantas filas como posibles propósitos de un viaje, y tantas columnas como rangos de edad habíamos distinguido en dicho DataFrame más una (la del propósito del viaje). Las columnas deben llamarse igual que cada uno de los niveles posibles de rangos de edad. Cada casilla del nuevo DataFrame deberá contener el **porcentaje** del número de accidentes ocurridos en ese tipo de viaje (fila) para ese rango de edad (columna), medido sobre el *total de accidentes ocurridos para ese tipo de viaje*.

// Pista: se puede hacer todo en una sola secuencia de transformaciones sin variable auxiliar. Calcular primero el conteo, después añadir una columna con los totales de cada tipo de viaje como la suma de las 5 columnas de conteos, y finalmente reemplazar cada columna de conteo por su porcentaje. No debe utilizarse `when` en ningún momento, solo aritmética de columnas. Recuerda cómo desplegar grupos en varias columnas.

val viajesPorEdadDF = accidentesAgeDF.select(col("Driver_Journey_Purpose"), col("Age_of_Driver"))
                                      .groupBy("Driver_Journey_Purpose")
                                      .pivot("Age_of_Driver")
                                      .count()
                                      .withColumn("total_accidentes_edad", 
                                                    col("Adolescente") + 
                                                    col("Adulto") + 
                                                    col("Joven") + 
                                                    col("7") + 
                                                    col("8"))
                                      .withColumn("Adolescente", col("Adolescente") / col("total_accidentes_edad"))
                                      .withColumn("Adulto", col("Adulto") / col("total_accidentes_edad"))
                                      .withColumn("Joven", col("Joven") / col("total_accidentes_edad"))
                                      .withColumn("7", col("7") / col("total_accidentes_edad"))
                                      .withColumn("8", col("8") / col("total_accidentes_edad"))
                                      .select(
                                              col("Driver_Journey_Purpose"),
                                              col("Adolescente"),
                                              col("Adulto"),
                                              col("Joven"),
                                              col("7"),
                                              col("8"),
                                              col("total_accidentes_edad")
                                        )
                                   



viajesPorEdadDF.show()



// COMMAND ----------

assert(viajesPorEdadDF.columns.length >= 6)
assert(viajesPorEdadDF.columns.contains("7"))
assert(viajesPorEdadDF.columns.contains("8"))
assert(viajesPorEdadDF.columns.contains("Adolescente"))
assert(viajesPorEdadDF.columns.contains("Joven"))
assert(viajesPorEdadDF.columns.contains("Adulto"))
assert(viajesPorEdadDF.columns(0) == "Driver_Journey_Purpose")
assert(viajesPorEdadDF.count == 5)
val commuting = (viajesPorEdadDF.orderBy("Driver_Journey_Purpose").collect())(0)
assert(commuting.toString.contains("Commuting"))
assert((commuting(4).toString.toDouble - 0.012527948326649396).abs < 0.001) // 7
assert((commuting(5).toString.toDouble - 0.002519785640770).abs < 0.001) // 8 
assert((commuting(1).toString.toDouble - 0.236327501153423).abs < 0.001) //adolescente
assert((commuting(2).toString.toDouble - 0.2791993469851297).abs < 0.001) //adulto
assert((commuting(3).toString.toDouble - 0.46942541789402703).abs < 0.001) //joven

// COMMAND ----------

// Unir la información obtenida en el paso anterior al DataFrame `accidentesAgeDF`, de manera que **al resultado final se añada una columna nueva llamada `Porcentaje`** que contenga el porcentaje de accidentes que ha habido para ese rango de edad y ese tipo de viaje de entre todos los viajes que ha habido de ese tipo (es decir, el porcentaje adecuado de la tabla anterior). Por ejemplo, si el accidente se produjo en un trayecto de tipo `Commuting...` y la persona es `Joven`, entonces la columna Porcentaje tomará el valor de la columna `Joven` y por tanto tendrá el valor 0.46942, pero si la persona es `Adulto`, entonces tomará el valor de la columna `Adulto` el cual será 0.2791993469851297.

// PISTA: unir los dos DF mediante join() convencional, y a continuación, crear la nueva columna `Porcentaje` en el resultado, utilizando `when` para ver cuál es el valor que debe tener en cada fila (más concretamente: de qué columna debemos tomarlo) en función del valor de la columna `Age_of_Driver`. No se necesitan variables intermedias; se puede hacer en una secuencia de transformaciones encadenadas.

viajesPorEdadDF.show()
accidentesAgeDF.show(200, 200, vertical = true)


val finalDF = viajesPorEdadDF.join(accidentesAgeDF, "Driver_Journey_Purpose")
                             .withColumn("Porcentaje", when(col("Age_of_Driver") === "Adolescente"  ,  col("Adolescente"))
                                                      .when(col("Age_of_Driver") === "Joven"  ,  col("Joven"))
                                                      .when(col("Age_of_Driver") === "Adulto"  ,  col("Adulto"))
                                                      .when(col("Age_of_Driver") === "7"  ,  col("7"))
                                                      .when(col("Age_of_Driver") === "8"  ,  col("8"))
                                        )
        



//Se hace join por Driver Journey Purpose 

// COMMAND ----------

finalDF.select("Driver_Journey_Purpose", "Age_of_Driver", "Porcentaje").show()


// COMMAND ----------

val prueba =  finalDF.where(col("Age_of_Driver") === "Adolescente").select(sum(col("Porcentaje")).alias("porcentaje")).collect
prueba(0)//

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def sum_cond(df: DataFrame, column: String, condition: Column )  = {

  val respuesta = df.where(condition).select(sum(column).alias(column)).collect()
  val casteo = respuesta(0)
  val cadena = casteo(0).toString.toFloat
  cadena
}

// COMMAND ----------

assert(finalDF.columns.contains("Porcentaje"))
assert((sum_cond(finalDF, "Porcentaje",col("Age_of_Driver") === "Adolescente").abs) - 13344.826819125037 < 0.001)
assert((sum_cond(finalDF, "Porcentaje",col("Age_of_Driver") === "Joven").abs) - 44438.00809518224 < 0.001)
assert((sum_cond(finalDF, "Porcentaje",col("Age_of_Driver") === "Adulto").abs) - 18028.24488479408 < 0.001)
assert((sum_cond(finalDF, "Porcentaje",col("Age_of_Driver") === "7").abs) - 812.0952970292334 < 0.001)
assert((sum_cond(finalDF, "Porcentaje",col("Age_of_Driver") === "8").abs) - 432.2987413617681 < 0.001)