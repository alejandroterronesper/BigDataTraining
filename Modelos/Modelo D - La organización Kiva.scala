// Databricks notebook source
// Ejercicio 1

// * Leerlo **sin intentar** que Spark infiera el tipo de dato de cada columna
// * Puesto que existen columnas que contienen una coma enmedio del valor, en esos casos los valores vienen entre comillas dobles. Spark ya contempla esta posibilidad y puede leerlas adecuadamente **si al leer le indicamos las siguientes opciones adicionales** además de las que ya sueles usar: `.option("quote", "\"").option("escape", "\"")`.
// * Asegúrate de que las **filas que no tienen el formato correcto sean descartadas**, indicando también la opción `mode` con el valor `DROPMALFORMED` como vimos en clase.
// * Crear un nuevo DF `kivaRawNoNullDF` en el que se hayan eliminado todas las filas que tengan algún valor nulo en cualquier columna **excepto en la columna tags**, que no será relevante para el análisis y por tanto podemos ignorar sus valores nulos y mantener dichas filas.

val rutaCSV = "/FileStore/tables/kiva_loans.csv"

val kivaRawDF = spark.read.format("csv")
                            .option("inferSchema", "false") //qure no infiera
                            .option("header", "true")
                            .option("delimiter",",")
                            .option("quote", "\"")
                            .option("escape","\"")
                            .option("mode", "DROPMALFORMED") //las filas sin formato correcto, se descartan
                            .load(rutaCSV)




var columnasExceptoTags = kivaRawDF.columns
columnasExceptoTags = columnasExceptoTags.filter(_!= "tags")


//Recorrer el dataframe, a partir de los campos que he filtrado en la variable
//y a partir de ellos elimino filas que sean null, a excepción de la de tags

// /buscar metodo que solo borre null no nan

val kivaRawNoNullDF = kivaRawDF.na.drop(columnasExceptoTags)
// kivaRawNoNullDF = kivaRawNoNullDF.where($"tags".isNull)


kivaRawNoNullDF.show(500, 500, vertical = true)

// COMMAND ----------

columnasExceptoTags.contains("tags")


// COMMAND ----------

kivaRawNoNullDF.count

// COMMAND ----------

assert(kivaRawNoNullDF.count == 574115)

// COMMAND ----------

kivaRawNoNullDF.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

// **(1 punto)** Ejercicio 2

// * Las columnas `posted_time` y `disbursed_time` son en realidad instantes de tiempo que Spark debería procesar como timestamp. Partiendo de `kivaRawNoNullDF`, reemplaza **ambas columnas** por su versión convertida a timestamp, utilizando `withColumn` con el mismo nombre de cada columna, y donde el nuevo valor de la columna viene dado por el siguiente código:

//         F.from_unixtime(F.unix_timestamp('nombreColumna', 'yyyy-MM-dd HH:mm:ssXXX')).cast("timestamp"))
// * Además, convierte a `DoubleType` la columna `loan_amount` y a `IntegerType` la columna `term_in_months`.

// * El DF resultante de todas estas operaciones debe quedar almacenado en la variable `kivaDF`, **cacheado**.


val kivaDF = kivaRawNoNullDF.withColumn("posted_time", to_timestamp(
                                                                      col("posted_time"), "yyyy-MM-dd HH:mm:ssXXX"
                                                                    )
                                        )
                            .withColumn("disbursed_time", to_timestamp(
                                                                      col("disbursed_time"), "yyyy-MM-dd HH:mm:ssXXX"
                                                                    ))
                            .withColumn("loan_amount", col("loan_amount").cast("double"))
                            .withColumn("term_in_months", col("term_in_months").cast("int"))
                            .cache()


// COMMAND ----------

val typesDict = kivaDF.dtypes
assert(typesDict(11)._2 == "TimestampType")
assert(typesDict(12)._2 == "TimestampType")
assert(typesDict(2)._2 == "DoubleType")
assert(typesDict(14)._2 == "IntegerType")
// cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
// nullsRow = kivaDF.select(cnt_cond(F.col("posted_time").isNull()).alias("posted_nulls"),
//               cnt_cond(F.col("disbursed_time").isNull()).alias("disbursed_nulls")).head()
// assert(nullsRow.posted_nulls == 0)
// assert(nullsRow.disbursed_nulls == 0)

// COMMAND ----------

kivaDF.printSchema

// COMMAND ----------

kivaDF.show(500, 500, vertical = true)

// * posted_time: Hora a la que el agente de campo (intermediario) publica el préstamo en Kiva
// * disbursed_time: Hora en que el agente de campo (intermediario) entrega el préstamo al beneficiario
// * funded_time: El momento en que el préstamo publicado en Kiva es financiado por los prestamistas por completo

// COMMAND ----------

// **(2 puntos)** Ejercicio 3

// Partiendo de `kivaDF`:

// * Primero, añade una nueva columna `dias_desembolso` que contenga el número de días que han pasado entre la fecha en que los prestamistas aceptaron financiar un proyecto, y la fecha en que el agente de campo entregó los fondos al beneficiario. Para ello, utiliza `withColumn` en combinación con la función `F.datediff("colFuturo", "colPasado")`
// * De manera análoga, añade otra nueva columna `dias_aceptacion` que contenga el número de días entre el anuncio de la necesidad de préstamo y la aceptación de financiarlo por parte de algún prestamista.
// * Reemplazar la columna `sector` por otra en la que se hayan traducido las categorías "Education" por "Educacion" (sin tilde para evitar posibles problemas) y "Agriculture" por "Agricultura", dejando como están el resto de categorías. **La sustitución no debe tener más que tres casos**: uno para cada categoría que vamos a reemplazar, y un tercero para el resto de categorías, que deben quedarse como estaban.
// * El resultado debe quedar guardado en la variable `kivaTiemposDF`.

val kivaTiemposDF = kivaDF.withColumn("dias_desembolso", datediff(col("posted_time"), col("disbursed_time")))
                          .withColumn("dias_aceptacion", datediff(col("funded_time"), col("posted_time"))) //funded time es de tipo string pero le vale al datediff
                          .withColumn("sector", when ((col("sector") === "Education"), lit("Educacion"))
                                                .when ((col("sector") === "Agriculture"), lit("Agricultura"))
                                                .otherwise(col("sector"))
                                      )
kivaTiemposDF.select("dias_desembolso", "dias_aceptacion", "sector").show()


// COMMAND ----------

// assert(kivaTiemposDF.where("sector == 'Agricultura'").count() == 157003)
// assert(kivaTiemposDF.where("sector == 'Educacion'").count() == 28417)
// # Comprobamos que las 13 restantes se mantienen sin cambios
// assert(kivaTiemposDF.groupBy("sector").count().join(kivaDF.groupBy("sector").count(), ["sector", "count"]).count() == 13)

// COMMAND ----------

kivaTiemposDF.show(500,500, vertical = true)

// COMMAND ----------

// **(3 puntos)** Ejercicio 4

// Partiendo de `kivaTiemposDF`, crear un nuevo DataFrame llamado `kivaAgrupadoDF` que tenga:

// * Tantas filas como **países (`country`; no usar el código de país)**, y tantas columnas como **sectores** (cada una llamada como el sector) más una (la columna del país, que debe aparecer en primer lugar). En cada celda deberá ir el número **medio (redondeado a 2 cifras decimales)** de días transcurridos en ese país y sector *entre la fecha en que se anuncia la necesidad de préstamo y la fecha en la que un prestamista acepta financiarlo*. Esta columna ha sido calculada en la celda precedente.
// * Después de esto, añadir una columna adicional `transcurrido_global` con el número **medio (redondeado a 2 cifras decimales) de días transcurridos en cada país** entre ambas fechas *sin tener en cuenta el sector*. Cada fila tendrá la media de las 15 columnas del apartado anterior.
// * Por último, ordenar el DF resultante **descendentemente** en base al tiempo medio global, `transcurrido_global`. El DF resultado de la ordenación debe ser almacenado en la variable `kivaAgrupadoDF`. 

// PISTA: utiliza el método `pivot` con el sector para el primer apartado, envolviendo a la función de agregación con la función `F.round`, es decir, `F.round(F.funcionAgregacion(....), 2)`, y `withColumn` con una operación aritmética entre columnas en el segundo. **No debe utilizarse la función `when`** ya que Spark es capaz de hacer directamente aritmética entre objetos columna. La operación aritmética también debe estar envuelta por round: `F.round(op. aritmética entre objetos columna, 2)`.



var kivaAgrupadoDF = kivaTiemposDF.groupBy(col("country"))
                                  .pivot(col("sector"))
                                  .agg( 
                                        round(
                                                avg(col("dias_desembolso")), 2
                                              )
                                       )

val columnasSinPais = kivaAgrupadoDF.columns.filter(_ != "country")


     kivaAgrupadoDF = kivaAgrupadoDF.withColumn("transcurrido_global", round (
                                                                            columnasSinPais.map(col(_)).reduce(_ + _)
                                                                            / columnasSinPais.length,2
                                                                            )
                                              ) //iteramos columnas y con el reduce sumo todas las convcinaciones 
                                              .orderBy(col("transcurrido_global"))



kivaAgrupadoDF.show(500,500, vertical = true)


// COMMAND ----------

 kivaAgrupadoDF.filter( col("country") === "United stated").show()
// assert(r1.country == "United States")
// assert((r1.Agricultura - 12.0 < 0.01) | (r1.Agricultura - 12.17 < 0.01))
// assert((r1.Educacion - 15.21 < 0.01) | (r1.Educacion - 15.33 < 0.01))
// assert(r1.Wholesale - 27.5 < 0.01)
// assert((r1.transcurrido_global - 20.94 < 0.01) | (r1.transcurrido_global - 21.04 < 0.01))

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
// **(3 puntos)** Ejercicio 5

// Partiendo de nuevo de `kivaTiemposDF`, añadir las siguientes columnas:

// * Primero, tres columnas adicionales llamadas `transc_medio`, `transc_min`, `transc_max` que contengan, respectivamente, **el número de días medio, mínimo y máximo transcurrido para proyectos de ese mismo país y ese mismo sector** entre la fecha en que se postea la necesidad de préstamo y la fecha en la que alguien acepta financiarlo (es decir, la columna `dias_aceptacion` calculada antes y utilizada también en la celda anterior). Es decir, queremos una columna extra para que podamos tener, junto a cada préstamo, información agregada de los préstamos similares, entendidos como aquellos del mismo país y del mismo sector. **No se debe utilizar JOIN sino solo funciones de ventana**.

val windowPaisSector = Window.partitionBy("sector", "country")


var kivaExtraInfoDF = kivaTiemposDF.withColumn("transc_medio", round(
                                                                      avg(col("dias_aceptacion")).over(windowPaisSector),2
                                                                      )
                                              )
                                   .withColumn("transc_min", min(col("dias_aceptacion")).over(windowPaisSector))
                                   .withColumn("transc_max", max(col("dias_aceptacion")).over(windowPaisSector))




// * Finalmente, crear otra columna adicional `diff_dias` que contenga la **diferencia en días entre los días que transcurrieron en este proyecto y la media de días de los proyectos similares** (calculada en el apartado anterior). Debería ser lo primero menos lo segundo, de forma que un número positivo indica que este préstamo tardó más días en ser aceptado que la media de días de este país y sector, y un número negativo indica lo contrario. El resultado debe obtenerse aplicando operaciones aritméticas con columnas existentes, **sin utilizar `when`**.

kivaExtraInfoDF = kivaExtraInfoDF.withColumn("diff_dias", round(
                                                                col("dias_aceptacion") - col("transc_medio"),2)
                                            )
// * El DF resultante con las 4 columnas nuevas que hemos añadido debe quedar almacenado en la variable `kivaExtraInfoDF`.

kivaExtraInfoDF.show(500,500, vertical = true)

// COMMAND ----------

val r = kivaExtraInfoDF.filter(col("id") === 658540)
r.head
// assert(r.country == 'Burkina Faso')
// assert(r.transc_medio - 11.02 < 0.05)
// assert(r.dias_aceptacion == 35)
// assert(r.diff_dias - 24.0 < 0.001)