// Databricks notebook source
val rutaCSV = "/FileStore/tables/Major_Contract_Awards.csv"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Ejercicio 1
// * Leerlo tratando de que Spark infiera el tipo de dato de cada columna, y **cachearlo**. 
// * Puesto que existen columnas que contienen una coma enmedio del valor, en esos casos los valores vienen entre comillas dobles. Spark ya contempla esta posibilidad y puede leerlas adecuadamente **si al leer le indicamos las siguientes opciones adicionales** además de las que ya sueles usar: `.option("quote", "\"").option("escape", "\"")`.
// * Asegúrate de que las **filas que no tienen el formato correcto sean descartadas**, indicando también la opción `mode` con el valor `DROPMALFORMED` como vimos en clase.


val contractsDF = spark.read.format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .option("delimiter",",")
                            .option("quote", "\"")
                            .option("escape","\"")
                            .option("mode", "DROPMALFORMED") //las filas sin formato correcto, se descartan
                            .load(rutaCSV)
contractsDF.cache()
contractsDF.show(10, 200, vertical = true)
contractsDF.printSchema

// COMMAND ----------


assert(contractsDF.count() == 148515)

// COMMAND ----------

// **(1 punto)** Ejercicio 2

// * La columna Total Contract Amount (USD) es en realidad numérica, pero todas las cantidades incluyen el signo `$` por lo que Spark la reconoce como string. Para corregir este comportamiento, vamos a eliminar el `$` de todas las filas utilizando la función `F.regexp_replace("Total Contract Amount (USD)", "\$", "")` donde `"\$"` es el string que queremos reemplazar (hay que escaparlo poniendo `\` delante porque sino el `$` se interpreta como un carácter especial), y siendo el nuevo string el string vacío, `""`. Esta función pertenece al paquete `pyspark.sql.functions`, por lo que ya funciona de manera distribuida, y devuelve como resultado un objeto columna transformado. 

// * Aplica esta función dentro de la función `withColumn` para **reemplazar** la columna `Total Contract Amount (USD)` ya existente por la columna devuelta por `regexp_replace`. La manera de utilizarla es totalmente análoga a la utilización de, por ejemplo, la función `F.when` dentro de `withColumn`. 
// * Aprovecha también para hacer un casting del objeto columna devuelto por regexp_replace, que es una columna de strings, a una columna de enteros: `F.regexp_replace(...).cast(...)`. Almacena el DF resultante en la variable `contractsDFenteros`, **cachéala** y utilízala a partir de este momento para trabajar en las celdas posteriores, salvo que la celda indique lo contrario.


var contractsDFenteros = contractsDF.withColumn("Total Contract Amount (USD)",
                                                  regexp_replace(col("Total Contract Amount (USD)"), "\\$", "").cast("int") //escapamos caracter especial
                                                )
contractsDFenteros.cache() //se guarda en cache
contractsDFenteros.printSchema
contractsDFenteros.show(10, 200, vertical = true)

// COMMAND ----------

assert(contractsDFenteros.columns.length == contractsDF.columns.length)
assert(contractsDFenteros.count() == contractsDF.count())
assert(contractsDFenteros.schema("Total Contract Amount (USD)").dataType == IntegerType)




// COMMAND ----------

val regionDF = contractsDFenteros.select("Region").distinct().toDF()


regionDF.show(false)

// COMMAND ----------

// * Partiendo de `contractsDFenteros`, crear un nuevo DF donde la columna "Region" sea reemplazada por otra con mismo nombre,
//  de tipo string en la que todos los valores de la columna original (LATIN AMERICA AND CARIBBEAN, SOUTH ASIA, OTHER ... etc)
// estén traducidos al español. Puedes elegir la traducción que más te guste, pero debe mantenerse el mismo número de categorías
// que ya había, que eran siete.

// * El evaluador oculto comprobará que sigue habiendo el mismo número de ejemplos en cada categoría con el nuevo nombre, y que las categorías efectivamente se han traducido (ninguna se debe llamar igual que antes). Puedes cambiar AFRICA por África.



val contractsTranslatedDF = contractsDFenteros.withColumn("Region", when (col("Region") === "LATIN AMERICA AND CARIBBEAN", "América Latina y caribe")
                                                    .when (col("Region") === "SOUTH ASIA", "Asia del Sur")
                                                    .when (col("Region") === "AFRICA", "África")
                                                    .when (col("Region") === "MIDDLE EAST AND NORTH AFRICA", "Oriente Medio y África del Norte")
                                                    .when (col("Region") === "EAST ASIA AND PACIFIC", "Asia del Este y Pacífico")
                                                    .when (col("Region") === "EUROPE AND CENTRAL ASIA", "Europa y Asia central")
                                                    .otherwise("Otro")
                                            )



// COMMAND ----------

assert(contractsTranslatedDF.select("Region").distinct().count() == 7)
val g1 = contractsDF.groupBy("Region").count().withColumnRenamed("Region", "R1").orderBy(col("count"))
val g2 = contractsTranslatedDF.groupBy("Region").agg(count("*").alias("c2")).orderBy(col("c2"))
val joinedDF = g1.join(g2, g1("count") === g2("c2"), "inner")
assert(joinedDF.count() == 7)
assert(joinedDF.where(col("count") === col("c2")).count() == 7)
assert(joinedDF.where(col("R1") === col("Region")).count() == 0)

// COMMAND ----------

// **(1 punto)** Ejercicio 4

// * Partiendo de `contractsTranslatedDF`, crear un nuevo DataFrame de **una sola fila** que contenga, **por este orden de columnas**, el **número** de categorías distintas existentes en cada una de las columnas `Procurement Type`, `Procurement Category` y `Procurement Method`. Pista: crear cada una de estas tres columnas al vuelo con `select`(). Renombrar cada columna de conteo para que se llame igual que la propia columna que estamos contando.




 val numeroCategoriasDF = contractsDFenteros.select(
                                        countDistinct(col("Procurement Type")).alias("Procurement Type") , //tienen que salir las distintas categorias, no valores repetidos  
                                        countDistinct(col("Procurement Category")).alias("Procurement Category") ,
                                        countDistinct(col("Procurement Method")).alias("Procurement Method") 
                                        )


// COMMAND ----------

assert(numeroCategoriasDF.columns.length == 3)
assert(numeroCategoriasDF.count() == 1)
val categorias = (numeroCategoriasDF.collect())(0)
assert(categorias(0).toString.toInt == 60)
assert(categorias(1).toString.toInt == 5)
assert(categorias(2).toString.toInt == 18)

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.Bucketizer

// Ejercicio 5

// * Partiendo de `contractsDFenteros` definido anteriormente, crear un **pipeline** formado por dos etapas: 
// un indexador de la columna categórica `Procurement Method` 
// y un discretizador (bucketizer) de la columna numérica que habíamos convertido a entero al principio, 
// `Total Contract Amount (USD)`, de manera que sea convertida en una columna de números reales empezando en 0 y cuya parte decimal siempre sea 0.

// * Para el indexador, si una vez entrenado le llegasen etiquetas que no ha visto antes,
// deberá eliminar esas filas (recordar la opción adecuada que vimos en clase). La columna de salida debe llamarse `ProcurementIndexed`.

// * Para el discretizador, utilizar como puntos de corte los siguientes:
//      (-Inf, 0, 100000, 200000, 300000, 400000, Inf). La columna de salida debe llamarse `TotalDiscretized`.


// * Una vez creados ambos, componerlos para crear un Pipeline, y aplicarlo a `contractsDFenteros`
//      para entrenar y a continuación también para predecir (es decir, transformarlo). 
//      El DF resultante de la transformación debe almacenarse en la variable `contractsTransformedDF`

//INDEXADOR
val miIndexador = new StringIndexer()
                                    .setInputCol("Procurement Method")
                                    .setOutputCol("ProcurementIndexed")
                                    .setHandleInvalid("skip")

//BUCKETIZER
val puntosCortes = Array(Double.NegativeInfinity, 0, 100000, 200000, 300000, 400000, Double.PositiveInfinity)


val miBucket = new Bucketizer ()
                              .setInputCol("Total Contract Amount (USD)")
                              .setOutputCol("TotalDiscretized")
                              .setSplits(puntosCortes)


val pipeline = new Pipeline()
                    .setStages(Array(miIndexador, miBucket))


// COMMAND ----------

val pipelineTransforma = pipeline.fit(contractsDFenteros)
val contractsTransformedDF = pipelineTransforma.transform(contractsDFenteros)



contractsTransformedDF.show(10, 200, vertical = true)


// COMMAND ----------

assert(contractsTransformedDF.columns.contains("TotalDiscretized"))
assert(contractsTransformedDF.columns.contains("ProcurementIndexed"))
assert(contractsTransformedDF.columns.length == (contractsDFenteros.columns.length + 2))

//isset, lift para acceder a posicion de array sin lanzar error
assert(pipeline.getStages.lift(0).isDefined)
assert(pipeline.getStages.lift(1).isDefined)
assert(contractsTransformedDF.schema("TotalDiscretized").dataType == DoubleType)
assert(contractsTransformedDF.schema("ProcurementIndexed").dataType == DoubleType)
var arrayPrueba = Array(Double.NegativeInfinity, 0.0, 100000.0, 200000.0, 300000.0, 400000.0, Double.PositiveInfinity)
assert(miBucket.getSplits.sameElements(arrayPrueba))
assert(miBucket.getInputCol == "Total Contract Amount (USD)")
assert(miBucket.getOutputCol == "TotalDiscretized")
assert(miIndexador.getInputCol == "Procurement Method")
assert(miIndexador.getOutputCol == "ProcurementIndexed")
assert(miIndexador.getHandleInvalid == "skip")


// COMMAND ----------

contractsDFenteros.show(500, 200, vertical = true)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// * Añadir una nueva columna al DF `contractsDFenteros` llamada `Total Pais` (sin tilde) que contenga en cada fila el **importe total prestado al país (Borrower Country Code) de esa fila (es decir, el total del país al que corresponde cada proyecto)**.
// El nuevo DF debe tener el mismo número de filas y una columna más. **NO DEBE UTILIZARSE JOIN sino funciones de ventana** con una ventana por país, que debe almacenarse en la variable `paisWindow`. El resultado sería equivalente a una agrupación por países y agregación de suma de importe, y después juntar cada total del país a cada celda (como si fuese un join por el país), pero **todo esto debe hacerse exclusivamente con ventanas y sin usar JOIN**.

val paisWindow = Window.partitionBy("Borrower Country Code")
val contractsDFenterosTPais = contractsDFenteros.withColumn("Total pais", 
                                                      (sum("Total Contract Amount (USD)"))
                                                      .over(paisWindow)
                                                      )




// Una vez hecho esto, añade una segunda columna nueva llamada `Porcentaje Pais` (sin tilde) que contenga el **porcentaje** que ha supuesto cada proyecto sobre el total destinado a ese país (dicho total ha sido calculado en el punto anterior). El porcentaje no debe ir en tanto por 1 sino en tanto por 100.

val contractsDFenterosPaisPorcentaje = contractsDFenterosTPais.withColumn("Porcentaje Pais", 
                                                                         ((col("Total Contract Amount (USD)") / 
                                                                                 col("Total pais")) * 100)  
                                                                          )


// contractsDFenterosPaisPorcentaje.show(500, 200, vertical = true)


// Añadir una tercera columna llamada `Media Pais` (sin tilde) que contenga en cada fila **el importe medio destinado a los proyectos del país al que corresponde el proyecto**. Debe utilizarse la misma ventana definida en el primer apartado, cambiando solo la función de agregación aplicada.

val contractsDFenterosMediaPais = contractsDFenterosPaisPorcentaje.withColumn("Media Pais",round( avg("Total Contract Amount (USD)")
                                                                                            .over(paisWindow), 2)
                                                                                          )
// contractsDFenterosMediaPais.show(500, 200, vertical = true)

// Añadir una cuarta columna llamada `Diff Porcentaje` que sea la diferencia, medida en porcentaje, entre el importe destinado al proyecto y el importe medio de un proyecto en ese país. Debe calcularse operando con las columnas existentes, restando a la columna `Total Contract Amount (USD)` el importe de `Media Pais`, dividiendo entre esta última y multiplicando por 100, **sin utilizar** en ningún caso la función `when`. Una diferencia positiva indicará que ese proyecto ha recibido más fondos que la media de los proyectos de ese país, y una diferencia negativa indicará lo contrario.

val porcentajesDF = contractsDFenterosMediaPais.withColumn("Diff Porcentaje", ((col("Total Contract Amount (USD)")
                                                                                - col("Media Pais"))
                                                                                 /col("Media Pais")) * 100
                                                                                
                                                          )
// El DF resultante debe almacenarse en una variable `porcentajesDF` y debe tener el mismo número de filas que `contractsDFenteros`.
porcentajesDF.show(500, 200, vertical = true)







// COMMAND ----------

porcentajesDF.printSchema

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

assert(porcentajesDF.columns.contains("Total pais"))
assert(porcentajesDF.columns.contains("Porcentaje Pais"))
val r = porcentajesDF.where("`Project ID` = 'P069947'").head()
assert(r(20) == 70485209)
assert((r(21).toString.toDouble - 25.985824912571374) < 0.001)
assert((r(22).toString.toDouble - 597332.279661017) < 0.001)
assert((r(23).toString.toDouble - 2966.3273396834217) < 0.001)


// COMMAND ----------

contractsTransformedDF.columns

// COMMAND ----------

prueba(21)

// COMMAND ----------

porcentajesDF.count()

// COMMAND ----------

contractsDFenteros.count()
