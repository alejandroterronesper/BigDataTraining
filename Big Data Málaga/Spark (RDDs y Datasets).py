# Databricks notebook source
# MAGIC %md
# MAGIC <h1 style= "color:red;">Índice API de Spark: Dataset </h1>
# MAGIC <h2>1. Rdds </h2>
# MAGIC <h3>Creación y Operaciones Básicas: </h3>

# COMMAND ----------

# Ejercicio 1: Crea un RDD a partir de una lista de números del 1 al 10 y aplica una operación
# map para obtener el cuadrado de cada número. Imprime el resultado.

# Importamos spark session
from pyspark.sql import SparkSession


# Creamos una lista del 1 al 10
listaNum = []

for cont in range(1,11):
    listaNum.append(cont)


#Creamos RDD con esos números
rddListaNumeros = sc.parallelize(listaNum)

#Usamos map para obtener el cuadrado de cada número
import math

rddCuadrado = rddListaNumeros.map(
                                lambda num: int(math.pow(num, 2))
                                )
# imprimo resultado                                
rddCuadrado.collect()


# COMMAND ----------

# Ejercicio 2: A partir de un archivo de texto, crea un RDD y cuenta el número de líneas que
# contiene.

rutaTexto = "/FileStore/tables/Enunciado.txt"
rddTexto = sc.textFile(rutaTexto)
print("Número de líneas " + str(rddTexto.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Transformaciones y Acciones:</h3>

# COMMAND ----------

# Ejercicio 3: Usa un archivo de texto que contenga varias líneas de texto. Crea un RDD y aplica
# una operación flatMap para dividir cada línea en palabras. Luego usa collect para imprimir
# todas las palabras.

ficheroTexto = "/FileStore/tables/ficheroTexto.txt"

#Creamos rdd a partir de fichero
rddTexto01 = sc.textFile(ficheroTexto)

#aplicamos flatMap y dividimos las cadenas por espacios
flatMapRdd = rddTexto01.flatMap(lambda cadena: cadena.split(" ")) #usamos flatMap porque split nos devuelve varios elementos


#Imprimimos 
for cadena in flatMapRdd.collect():
    print (cadena)


# COMMAND ----------

# Ejercicio 4: Crea un RDD con una lista de números del 1 al 100. Filtra los números pares y
# utiliza la acción count para obtener cuántos números pares hay.

listaNumeros = []

#cargamos lista
for num in range(1,101):
    listaNumeros.append(num)

#creamos RDD
rddNumeros = sc.parallelize(listaNumeros)


filtrarPares = rddNumeros.filter(lambda num: num % 2 == 0 )

print("Números pares: " + str(filtrarPares.count() ))

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Operaciones de Conjunto:
# MAGIC </h3>

# COMMAND ----------

# Ejercicio 5: Crea dos RDDs con listas de números diferentes. Realiza la operación de unión
# (union) y muestra el resultado.
from random import randint

cont = 1

lista1 = []
lista2 = []
while cont <= 10:
    lista1.append(randint(0, 100))
    lista2.append(randint(0, 100))
    cont += 1

rdd1 = sc.parallelize(lista1)
rdd2 = sc.parallelize(lista2)

rdd3 = rdd1.union(rdd2)

rdd3.collect()

# COMMAND ----------

rdd1.collect()


# COMMAND ----------

rdd2.collect()

# COMMAND ----------

# Ejercicio 6: Crea dos RDDs con listas de números diferentes. Realiza la operación de
# intersección (intersection) y muestra el resultado.

#usamos los rdds anteriores
rdd4 = rdd1.intersection(rdd2)
rdd4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Operaciones de agregación</h3>

# COMMAND ----------

# Ejercicio 7: A partir de un archivo de texto, crea un RDD y aplica una operación map para
# contar el número de caracteres de cada línea. Usa reduce para obtener el total de caracteres
# en el archivo.



# Saco un  de RDDs de palabras
rddLongitudLinea = rddTexto01.map(   lambda linea: len(linea)     )

rddLongitudLinea.collect()

totalCaracteres = rddLongitudLinea.reduce(lambda a,b: a+b)
    
print(totalCaracteres)

# COMMAND ----------

# Ejercicio 8: Crea un RDD a partir de una lista de tuplas (nombre, valor). Usa reduceByKey para
# sumar los valores de cada nombre y muestra el resultado.
from operator import add 


listaTuplas = [("alejandro", 26), ("carlos", 29), ("antonio", 34), ("alejandro", 20), ("antonio", 50)]

rddTuplas = sc.parallelize(listaTuplas)

totalValores = rddTuplas.reduceByKey(add)

for valor in totalValores.collect():
    print(valor)


# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Trabajo con Datos Estructurados:
# MAGIC <h3>

# COMMAND ----------

# Ejercicio 9: A partir de un archivo CSV con datos estructurados (por ejemplo, datos de ventas),
# crea un RDD y transforma los datos para obtener el total de ventas por cada categoría. Usa
# map y reduceByKey para lograrlo

salesData = [
                ("Electronics", 1000),
                ("Clothing", 500),
                ("Electronics", 1500),
                ("Clothing", 700),
                ("Furniture", 300)
            ]

rddVentas = sc.parallelize(salesData)

ventasCategoria = rddVentas.reduceByKey(add)

for valor in ventasCategoria.collect():
    print(valor)

# COMMAND ----------

# Ejercicio 10: Crea un RDD con datos de clientes (ID, nombre, edad). Filtra los clientes que
# tienen más de 30 años y muestra sus nombres utilizando la operación filter y map.

clientes = [
                (1, "Alice", 25),
                (2, "Bob", 35),
                (3, "Charlie", 30),
                (4, "David", 40)
            ]


rddClientes = sc.parallelize(clientes)

filtraEdad = rddClientes.filter(lambda cliente: cliente[2] > 30 ).map(lambda cliente: cliente[1])

filtraEdad.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>2. Dataset </h2>
# MAGIC <h3>Tareas: </h3>
# MAGIC <h4>Cargar el Dataset</h4>

# COMMAND ----------

# importamos libreria panda
import pandas as pd 

# 1. Cargar el Dataset: Lee el archivo CSV en un DataFrame de Spark.

rutaCSV = "/FileStore/tables/ventas_productos.csv"
miDF = spark.read.csv(rutaCSV, header=True)



# COMMAND ----------

display(miDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Análisis Descriptivo </h4>

# COMMAND ----------

from pyspark.sql.functions import col, split
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, split, sum as _sum

# 2. Análisis Descriptivo: Calcula las siguientes estadísticas descriptivas:
#  Total de ventas por día.
#  Producto más vendido en términos de cantidad.
#  Producto con mayores ingresos totales.

dfDia = miDF.withColumn("diasMes",split(col("Fecha"), "-")[2]) \
            .withColumn("Total_venta", col("Total_venta").cast(DoubleType()))

totalVentasDias = dfDia.groupBy("diasMes") \
                       .agg(_sum(col("Total_venta"))) \
                       .orderBy(col("diasMes"))

display(totalVentasDias)

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, sum as _sum

#  Producto más vendido en términos de cantidad.
cantidadDF = miDF.withColumn("Cantidad", col("Cantidad") \
                 .cast(IntegerType())) \
                 .groupBy("Producto","Cantidad") \
                 .count()




cantidadDF = cantidadDF.groupBy("Producto") \
                       .agg(_sum(col("Cantidad")) \
                       .alias("Total_ventas")) \
                       .orderBy(col("Producto"))

display(cantidadDF)

# COMMAND ----------

miDF.printSchema()

# COMMAND ----------

#  Producto con mayores ingresos totales.

ingresosGroup = miDF.withColumn("Total_venta",
                                col("Total_venta").cast(DoubleType())
                                ) \
                    .groupBy("Producto", "Total_venta") \
                    .count()
        
ingresosGroup = ingresosGroup.groupBy("Producto") \
                             .agg(_sum(col("Total_venta")) \
                                  .alias("Ingreso_total")
                                  ) \
                             .orderBy(col("Producto"))

display(ingresosGroup)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Visualización de Datos<h4>

# COMMAND ----------

# Crea gráficos de barras que muestren el total de ventas diarias
# y la cantidad vendida de cada producto


# pivotar la columa de los productos
dfPivotado = miDF.groupBy("Fecha").pivot("Producto").count()

dfGrafico = miDF.withColumn("Total_venta", 
                                            col("Total_venta").cast(DoubleType())
                            ) \
                            .groupBy("Fecha") \
                                .agg(_sum(col("Total_venta")).alias("Total_ventas_diarias"))



dfGrafico = dfGrafico.join(dfPivotado, on = "Fecha", how= "left").orderBy(col("Fecha"))
                 
            

display(dfGrafico)
