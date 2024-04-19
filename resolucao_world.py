from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df_world = spark.read.csv('/content/data/world.txt', sep = ";", header="True", inferSchema="True")

print('Tipo de dados das colunas:')
df_world.printSchema()

print('Tabela após a leitura:')
print(df_world.show())

df_world_filtered = df_world.filter(
      (F.col('população') >= 25000000) | (F.col('area') >= 3000000)
    ).select('nome', 'população', 'area')

print('Resolução: população de pelo menos 25.000.000 ou area de pelo menos 3.000.000')
print(df_world_filtered.show())
