from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W 

spark = SparkSession.builder.getOrCreate()
df_product = spark.read.csv('/content/data/products.txt', sep = ";", header="True", inferSchema="True")

print('Tipo de dados das colunas:')
df_product.printSchema()

print('Tabela após a leitura:')
print(df_product.show())

print('Separando a chave primaria da tabela e colocando o valor inicial de preço do produto')
df_product_transform = df_product.select('product_id').distinct().withColumn('first_price', F.lit(10))
print(df_product_transform.show())

print('Filtrando todos os valores dos produtos até a data 2019-08-16')
df_product_temp = df_product.filter(
     F.col('change_date') <= '2019-08-16'
 )
print(df_product_temp.show())

wi_rank_product = W().partitionBy('product_id').orderBy(F.desc('change_date'))
print('Rankeando os produtos anteriores a data 2019-08-16, mantendo o registro mais recente por id')
print('Assumindo o primeiro valor como verdadeiro caso não tenha preenchimento da coluna new_price')
df_product_temp = df_product_temp.withColumn(
    'rank', F.row_number().over(wi_rank_product)
    ).filter(
        F.col('rank') == 1
    ).join(
        df_product_transform, on='product_id',how='right'
    ).withColumn(
        'price', F.coalesce(F.col('new_price'), F.col('first_price'))
    ).select('product_id', 'price')

print(df_product_temp.show())