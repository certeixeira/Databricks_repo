# Databricks notebook source
# MAGIC %md
# MAGIC # ANÁLISE DOS REGISTROS DO SIHSUS NO PRIMEIRO SEMESTRE DE 2023

# COMMAND ----------

# MAGIC %md
# MAGIC O Sistema de Informações Hospitalares do SUS (SIH/SUS) é um sistema de registro administrativo responsável por transcrever todos os atendimentos provenientes de internações hospitalares que foram financiadas pelo SUS. Esta análise aborda apenas os aspectos relacionados à população registrada entre janeiro de 2023 até junho de 2023 no SIH/SUS, sem considerar dados regionais.

# COMMAND ----------

# MAGIC %md
# MAGIC ##imports

# COMMAND ----------

import plotly.express as px
from pyspark.sql.functions import col, when
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quem usa o SIHSUS?

# COMMAND ----------

# MAGIC %md
# MAGIC Analisando a distribuição racial dos registros podemos ver que a população Parda se destaca com 3.348.383, representando 55.45% do total populacional, seguido pela população Branca com um total de 2.260.623 (37.43%), Preta com 317.430 (5.26%), Amarela e Indígena com 92.472 (1.53%) e 19.905 (0.33%) respectivamente.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   rc.descRacaCor AS raca_cor,
# MAGIC   COUNT(rc.descRacaCor) AS total_populacional,
# MAGIC   ROUND(
# MAGIC     100.0 * COUNT(rc.descRacaCor) / SUM(COUNT(rc.descRacaCor)) OVER (),
# MAGIC     2
# MAGIC   ) AS porcentagem
# MAGIC FROM
# MAGIC   bronze.datasus.sihsus ss
# MAGIC   JOIN bronze.datasus.raca_cor rc ON rc.codRacaCor = ss.RACA_COR
# MAGIC WHERE
# MAGIC   ss.ANO_CMPT = '2023'
# MAGIC GROUP BY
# MAGIC   rc.descRacaCor
# MAGIC ORDER BY
# MAGIC   total_populacional;

# COMMAND ----------

df_frequencia = spark.sql(
    """
    SELECT 
    rc.descRacaCor AS raca_cor, 
    COUNT(rc.descRacaCor) AS frequencia,
    SUM(COUNT(rc.descRacaCor)) OVER (ORDER BY rc.descRacaCor) AS frequencia_acumulada,
    ROUND(100.0 * COUNT(rc.descRacaCor) / SUM(COUNT(rc.descRacaCor)) OVER (), 2)AS frequencia_relativa
    FROM 
    bronze.datasus.sihsus ss
    JOIN 
    bronze.datasus.raca_cor rc ON rc.codRacaCor = ss.RACA_COR
    WHERE ss.ANO_CMPT = '2023' 
    GROUP BY rc.descRacaCor
    ORDER BY frequencia;
    """
)

df_frequencia = df_frequencia.toPandas()


fig = px.bar(df_frequencia, y='raca_cor', x='frequencia',
             labels={'descRacaCor':'Raça', 'frequencia':'total'},
             title='Registros por Raça',
             text='frequencia')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Separando por mês
# MAGIC
# MAGIC *Mês de processamento da AIH (Autorização de Internação Hospitalar)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ss.MES_CMPT as mes,
# MAGIC     rc.descRacaCor AS raca_cor, 
# MAGIC     COUNT(rc.descRacaCor) AS frequencia
# MAGIC   FROM 
# MAGIC     bronze.datasus.sihsus ss
# MAGIC   JOIN 
# MAGIC     bronze.datasus.raca_cor rc ON rc.codRacaCor = ss.RACA_COR
# MAGIC   WHERE 
# MAGIC     ss.ANO_CMPT = '2023' 
# MAGIC   GROUP BY 
# MAGIC     ss.MES_CMPT, rc.descRacaCor
# MAGIC   ORDER BY 
# MAGIC     ss.MES_CMPT, frequencia;

# COMMAND ----------

dict_mes = {
    '1': 'Janeiro',
    '2': 'Fevereiro',
    '3': 'Março',
    '4': 'Abril',
    '5': 'Maio',
    '6': 'Junho'
}

# COMMAND ----------

df_racas_mes = spark.sql(
  """
  SELECT 
    ss.MES_CMPT as num_mes,
  CASE
    WHEN ss.MES_CMPT = 1 THEN 'Janeiro'
    WHEN ss.MES_CMPT = 2 THEN 'Fevereiro'
    WHEN ss.MES_CMPT = 3 THEN 'Março'
    WHEN ss.MES_CMPT = 4 THEN 'Abril'
    WHEN ss.MES_CMPT = 5 THEN 'Maio'
    WHEN ss.MES_CMPT = 6 THEN 'Junho'
  END AS mes,
    rc.descRacaCor AS raca_cor, 
    COUNT(rc.descRacaCor) AS frequencia
  FROM 
    bronze.datasus.sihsus ss
  JOIN 
    bronze.datasus.raca_cor rc ON rc.codRacaCor = ss.RACA_COR
  WHERE 
    ss.ANO_CMPT = '2023' 
  GROUP BY 
    ss.MES_CMPT, rc.descRacaCor
  ORDER BY 
    ss.MES_CMPT, frequencia;
  """
)

df = df_racas_mes.toPandas()
fig = px.line(df, y='frequencia', x='mes', color='raca_cor')
fig.update_layout(title='Frequência de ocorrência por raça e mês',
                  xaxis_title='Mês',
                  yaxis_title='Frequência',
                  legend_title='Raça')

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por sexo

# COMMAND ----------

# MAGIC %md
# MAGIC ###Geral

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.datasus.sihsus

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    sx.descSexo AS sexo,
# MAGIC     COUNT(*) AS frequencia,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS porcentagem
# MAGIC FROM
# MAGIC     bronze.datasus.sihsus ss
# MAGIC     JOIN bronze.datasus.sexo sx ON ss.SEXO = sx.codSexo
# MAGIC WHERE
# MAGIC     ss.ANO_CMPT = '2023'
# MAGIC GROUP BY
# MAGIC     sx.descSexo
# MAGIC ORDER BY
# MAGIC     sexo;

# COMMAND ----------

query = """
SELECT
raca_cor,
sexo,
frequencia,
ROUND(frequencia * 100.0 / total_por_raca, 2) AS porcentagem
FROM (
SELECT
    rc.descRacaCor AS raca_cor,
    sx.descSexo AS sexo,
    COUNT(rc.descRacaCor) AS frequencia,
    SUM(COUNT(rc.descRacaCor)) OVER (PARTITION BY rc.descRacaCor) AS total_por_raca
FROM
    bronze.datasus.sihsus ss
    JOIN bronze.datasus.raca_cor rc ON rc.codRacaCor = ss.RACA_COR
    JOIN bronze.datasus.sexo sx ON ss.SEXO = sx.codSexo
WHERE
    ss.ANO_CMPT = '2023'
GROUP BY
    rc.descRacaCor, sx.descSexo
) subquery
ORDER BY
raca_cor, sexo;
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por sexo e raça

# COMMAND ----------

df = spark.sql(query).toPandas()

#figs = []
#for raca in df['raca_cor'].unique():
#    df_raca = df[df['raca_cor'] == raca]
#
#    fig = px.bar(df_raca, x='sexo', y='porcentagem', color='sexo',
#                 title=f'Porcentagem de população por sexo - Raça: {raca}',
#                 labels={'porcentagem': 'Porcentagem', 'sexo': 'Sexo'})
#
#    for index, row in df_raca.iterrows():
#        fig.add_annotation(x=row['sexo'], y=row['porcentagem'], text=f"{row['porcentagem']}%",
#                           showarrow=False, font=dict(size=12, color='black'), xshift=0)
#
#    figs.append(fig)
#
#for fig in figs:
#    fig.show()

# COMMAND ----------

fig = px.bar(df, x='sexo', y='frequencia', color='raca_cor'
             , title="Distribuição do Sexo por Raça")
fig.show()

# COMMAND ----------

fig = px.bar(df,  x='raca_cor', y='porcentagem', color='sexo', barmode='group', text='porcentagem')
fig.update_layout(title='Porcentagem de Internações por Raça e Sexo',
                  xaxis_title='Raça',
                  yaxis_title='porcentagem')
fig.show()

# COMMAND ----------

fig = px.bar(df,  x='raca_cor', y='frequencia', color='sexo', barmode='group', text='frequencia')
fig.update_layout(title='Frequência de Internações por Raça e Sexo',
                  xaxis_title='Raça',
                  yaxis_title='Frequência')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INVESTIMENTO NO SIHSUS NO PRIMEIRO SEMESTRE DE 2023

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(ss.VAL_TOT)) AS total
# MAGIC FROM bronze.datasus.sihsus ss
# MAGIC WHERE ss.ANO_CMPT = "2023"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agrupando por mês

# COMMAND ----------

query = """
SELECT
  ss.MES_CMPT as num_mes,
  CASE
    WHEN ss.MES_CMPT = 1 THEN 'Janeiro'
    WHEN ss.MES_CMPT = 2 THEN 'Fevereiro'
    WHEN ss.MES_CMPT = 3 THEN 'Março'
    WHEN ss.MES_CMPT = 4 THEN 'Abril'
    WHEN ss.MES_CMPT = 5 THEN 'Maio'
    WHEN ss.MES_CMPT = 6 THEN 'Junho'
  END AS mes,
  ROUND(SUM(ss.VAL_TOT)) AS total
FROM
  bronze.datasus.sihsus ss
WHERE
  ss.ANO_CMPT = "2023"
GROUP BY
  num_mes
ORDER BY
  num_mes"""


df = spark.sql(query).toPandas()
fig = px.line(df, x="mes", y="total", title="Total gasto por Mês")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agrupando por sexo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sx.descSexo AS sexo ,ROUND(SUM(ss.VAL_TOT), 2) AS total
# MAGIC FROM bronze.datasus.sihsus ss
# MAGIC JOIN bronze.datasus.sexo sx ON ss.SEXO = sx.codSexo
# MAGIC WHERE ss.ANO_CMPT = "2023"
# MAGIC GROUP BY sx.descSexo

# COMMAND ----------

total_por_sexo = spark.sql(
    """
    SELECT sx.descSexo AS sexo ,ROUND(SUM(ss.VAL_TOT)) AS total
    FROM bronze.datasus.sihsus ss
    JOIN bronze.datasus.sexo sx ON ss.SEXO = sx.codSexo
    WHERE ss.ANO_CMPT = "2023"
    GROUP BY sx.descSexo
    """
)
total_por_sexo = total_por_sexo.toPandas()
fig = px.bar(total_por_sexo, x="sexo", y="total", color='sexo')
fig.show()

# COMMAND ----------

df = spark.sql("""
SELECT
  ss.MES_CMPT as num_mes,
  CASE
    WHEN ss.MES_CMPT = 1 THEN 'Janeiro'
    WHEN ss.MES_CMPT = 2 THEN 'Fevereiro'
    WHEN ss.MES_CMPT = 3 THEN 'Março'
    WHEN ss.MES_CMPT = 4 THEN 'Abril'
    WHEN ss.MES_CMPT = 5 THEN 'Maio'
    WHEN ss.MES_CMPT = 6 THEN 'Junho'
  END AS mes,
  sx.descSexo AS sexo,
  ROUND(SUM(ss.VAL_TOT)) AS total
FROM
  bronze.datasus.sihsus ss
  JOIN bronze.datasus.sexo sx ON sx.codSexo = ss.SEXO
WHERE
  ss.ANO_CMPT = '2023'
GROUP BY
  ss.MES_CMPT,
  sx.descSexo
ORDER BY
  num_mes,
  sx.descSexo;""").toPandas()

fig = px.line(df, y='total', x='mes', color='sexo')
fig.update_layout(title='Gasto mensal por sexo',
                  xaxis_title='Mês',
                  yaxis_title='Total',
                  legend_title='Sexo')

fig.show()

# COMMAND ----------

df = spark.sql("""
SELECT
  FLOOR(ss.IDADE / 5) * 5 AS faixa_etaria,
  ROUND(SUM(ss.VAL_TOT)) AS total,
  ROUND(AVG(ss.VAL_TOT)) AS media
FROM
  bronze.datasus.sihsus ss
GROUP BY
  FLOOR(ss.IDADE / 5) * 5
ORDER BY
  faixa_etaria;
""").toPandas()

fig = px.line(df, y='total', x='faixa_etaria')

fig.show()

# COMMAND ----------


