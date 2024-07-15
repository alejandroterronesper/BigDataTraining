// Databricks notebook source
// MAGIC %md
// MAGIC <h3 style="color:red;">Generar tres funciones:  </h3>
// MAGIC   <li>  Contar el count, si falla dar mensaje de aviso </li>
// MAGIC   <li> Contar el número de columnas</li>
// MAGIC   <li> Sacar diferencias entre dataframes (devolver otro dataframe)  </li>
// MAGIC   

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val inputDf: DataFrame = spark.createDataFrame(
    Seq(
          ("1", "ESP", "Murex3", "2023-04-13", "7388.32", "A189303", "2023-05-31"),
          ("2", "UK", "Murex3", "2023-02-19", "2948.00", "A049248", "2023-05-31"),
          ("3", "USA", "GBO", "2023-04-21", "346314.34", "A134099", "2023-05-31"),
          ("4", "HK", "AMD", "2023-03-02", "837.05", "A826457", "2023-05-31"),
          ("5", "FRA", "Murex3", "2023-05-30", "150.85", "A939398", "2023-05-31"),
          ("6", "ITA", "Murex3", "2023-04-22", "560.86", "A027438", "2023-05-31"),
          ("7", "RUM", "Murex3", "2023-01-30", "7432.82", "A980258", "2023-05-31"),
          ("8", "ESP", "Murex3", "2023-02-28", "800.56", "A829578", "2023-05-31"),
          ("9", "RUS", "RSM", "2023-04-1", "432.89", "A924834", "2023-05-31"),
          ("10", "ARG", "GBO", "2023-02-17", "1398.455", "A0897344", "2023-05-31")
        )
      ).toDF("id", "geo", "source", "executionDate", "amount", "user", "dataDatePart")

val expected = spark.createDataFrame(
  Seq(
        ("1", "ESP", "Murex3", "2023-04-13", "7388.32", "2023-05-31"),
        ("2", "UK", "Murex3", "2023-02-19", "2948.00", "2023-05-31"),
        ("3", "USA", "GBO", "2023-04-21", "346314.34", "2023-05-31"),
        ("4", "HK", "AMD", "2023-03-02", "837.05", "2023-05-31"),
        ("5", "FRA", "Murex3", "2023-05-30", "150.85", "2023-05-31"),
        ("6", "ITA", "Murex3", "2023-04-22", "560.86", "2023-05-31"),
        ("7", "RUM", "Murex3", "2023-01-30", "7432.82", "2023-05-31"),
        ("8", "ESP", "Murex3", "2023-02-28", "800.56", "2023-05-31"),
        ("10", "ARG", "GBO", "2023-02-17", "1398.455", "2023-05-30")
      )
  ).toDF("id", "geo", "source", "executionDate", "amount", "dataDatePart")


  

// COMMAND ----------

def cuentaDF (d1: DataFrame, d2 : DataFrame) = {

  d1.cache()
  d2.cache()

  if (d1.count() != d2.count()){
    "El count de los dataframes es distinto"
  }

}


// COMMAND ----------

cuentaDF(inputDf, expected)

// COMMAND ----------


def cuentaColumnasDF (d1: DataFrame, d2 : DataFrame) = {

  if (d1.columns.length != d2.columns.length){
    "Los dataframes tienen número distinto de columnas"
  }
}

// COMMAND ----------

cuentaColumnasDF(inputDf, expected)

// COMMAND ----------

// Los df son distintos, le añadimos la columna que el falta a uno
import org.apache.spark.sql.functions.lit
val newDF = expected.withColumn("user", lit("usuario"))

// COMMAND ----------

newDF

// COMMAND ----------

def difDataFrame (d1: DataFrame, d2 : DataFrame) : DataFrame = {

  val nuevoDF = d1.except(d2)
  nuevoDF
}

// COMMAND ----------

val diferenciaDF = difDataFrame(inputDf, newDF)
diferenciaDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>  Se pide hacer una udf que te permita poner las mayusculas en el campo message </h3>

// COMMAND ----------

val inputDf: DataFrame = spark.createDataFrame(
    Seq(
          ("1", "y empezó a recitar de repente. en un lugar de la mancha, de cuyo nombre no quiero acordarme", "2023-08-06"),
          ("2", "buenos días dijo el amable camarero. tomaré lo de siempre, dijo el cliente", "2023-08-06"),
          ("3", "el resultado del partido fue 0-2. un mal partido que puede certificar el descenso del equipo", "2023-08-06"),
          ("4", "creo que sí, estás loco. pero te diré un secreto: las mejores personas lo están", "2023-08-06"),
          ("5", "sólo un hombre que ha sentido la máxima desesperación es capaz de sentir la máxima felicidad. es necesario haber deseado morir para saber lo bueno que es vivir", "2023-08-06")
        )
      ).toDF("id", "message", "dataDatePart")



// Expected result
val expected: DataFrame = spark.createDataFrame(
        Seq(
              ("1", "Y empezó a recitar de repente. En un lugar de la mancha, de cuyo nombre no quiero acordarme", "2023-08-06"),
              ("2", "Buenos días dijo el amable camarero. Tomaré lo de siempre, dijo el cliente", "2023-08-06"),
              ("3", "El resultado del partido fue 0-2. Un mal partido que puede certificar el descenso del equipo", "2023-08-06"),
              ("4", "Creo que sí, estás loco. Pero te diré un secreto: las mejores personas lo están", "2023-08-06"),
              ("5", "Sólo un hombre que ha sentido la máxima desesperación es capaz de sentir la máxima felicidad. Es necesario haber deseado morir para saber lo bueno que es vivir", "2023-08-06")
            )
            ).toDF("id", "message", "dataDatePart")

// COMMAND ----------

import org.apache.spark.sql.functions.udf


val cadenaMayus = udf((cadena: String) => {
    // Verificar que la cadena no sea nula
    if (cadena != null) {
        // Dividir la cadena en palabras
        val palabras = cadena.toLowerCase.split("\\s+")

        // Capitalizar la primera palabra
        if (palabras.nonEmpty) {
            palabras(0) = palabras(0).capitalize
        }

        // Reconstruir la cadena capitalizada
        var cadenaCapitalizada = palabras.mkString(" ")

        // Aplicar expresión regular para detectar puntos seguidos de una letra y capitalizarla
        val patron = """\. ([a-zA-Z])""".r
        cadenaCapitalizada = patron.replaceAllIn(cadenaCapitalizada, m => ". " + m.group(1).toUpperCase)

        cadenaCapitalizada
    } else {
        // Si la cadena es nula, devolver nulo
        null
    }
})

// COMMAND ----------

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val dfMayuscula = inputDf.withColumn("message", cadenaMayus(col("message")))

// COMMAND ----------

assert(expected.collect() sameElements dfMayuscula.collect())
