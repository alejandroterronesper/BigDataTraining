// Databricks notebook source
//guardamos ruta
val rutaJSON = "/FileStore/tables/jsonMessage.json"
//https://docs.databricks.com/en/_extras/notebooks/source/kb/scala/nested-json-to-dataframe.html

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

final val trueFlag: Boolean = true



// Input data
val dfInput: DataFrame = spark.read.option("multiline", trueFlag).json(rutaJSON)



// Expected result
val expected: DataFrame = spark.createDataFrame(Seq(
  ("3833", "2024-03-14", "1", "24084239", "149.99", "118.79", "268.78"),
  ("3833", "2024-03-14", "1", "27240258", "260", "433.99", "693.99"),
  ("3833", "2024-03-14", "1", "35257449", "645", "823.23", "1468.23")))
  .toDF("resort", "businessDateJson", "isCore", "clientIdJson", "roomRevenue", "board", "totalRevenue")



// COMMAND ----------

dfInput.show(false)
expected.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

var respuesta :DataFrame = dfInput.select(
                                col("resort"),
                                col("businessDate"),
                                col("isCore"),
                                explode(col("revenue"))
                              )
                        .withColumn("clientId", $"col.clientId")   
                        .withColumn("roomRevenue", $"col.roomRevenue")   
                        .withColumn("board", $"col.board")   
                        .withColumn("totalRevenue", $"col.totalRevenue")
                        .drop(col("col"))
       

// COMMAND ----------

respuesta.show()

// COMMAND ----------

respuesta.printSchema
expected.printSchema

// COMMAND ----------

assert(respuesta.collect() sameElements expected.collect())


// COMMAND ----------

//Optimización 

var otraManera = dfInput

// COMMAND ----------

otraManera.printSchema

// val nombreCols = otraManera.columns

// COMMAND ----------

//array de con structfield
val campos = otraManera.schema.fields

// COMMAND ----------

val nombresCampos = campos.map(campo => campo.name)

// COMMAND ----------

//Vamos a hacer lo siguiente, como yo no se cuantos anidamientos hay dentro del dataframe
//lo que hago es iterar sus campos y preguntar por el tipo array
//si es tipo array, itero ese campo y hago select a las columnas que contiene


var arrayTipo = ArrayType(StringType, containsNull = true) //Declaramos campos vacios
var arrayStructFiled = StructField("arrayField", arrayTipo, nullable = true) //Aqui es donde vamos a guardar nuestro StructField

for (pos <- campos.indices){

        var campo = campos(pos)
        var campoTipo = campo.dataType
        var campoNombre = campo.name

        campoTipo match {

            case stringTipo: StringType => {}

            case arrayTipo: ArrayType => {
                var camposArray = nombresCampos.filter( _ != campoNombre)
                print(camposArray + "     dsfsdf")
            }


        }

        //   if (campos(pos).dataType.isInstanceOf[ArrayType] == true){ //Filtramos por lo que son de tipo array
       
        //      arrayStructFiled = campos(pos)


        //   }
}

arrayStructFiled

// COMMAND ----------

camposArray

// COMMAND ----------

structFieldArrayEmpty

// COMMAND ----------

arrayStructFiled.fields.foreach{
  campo => println(${campo.name}) 
}