import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark.sql.types import StringType
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("RimacHackathon").getOrCreate()

# Define the paths
large_csv_path = 's3://acul/df_ambulatorio_nasty.csv'
catalogo_cie10_path = 's3://diccionarios/CATALOGO_CIE10.csv'
save_directory = 's3://limpio/csv/save_directory'

# Ensure the save directory exists
if not os.path.exists(save_directory):
    os.makedirs(save_directory, exist_ok=True)

# Load the disease catalog
cat_diseases = pd.read_csv(catalogo_cie10_path)
cat_diseases_dict = dict(zip(cat_diseases['CODIGOCIE10'], cat_diseases['NOMBRECIE10']))

# Load the CSV file
df = spark.read.csv(large_csv_path, header=True, inferSchema=True, encoding='latin-1')

# Drop unnecessary columns
df = df.drop('CATEGORIA', 'CO_IPRESS', 'UBIGEO')

# Define the mappings
age_mapping = {
    '1': 'Menores de 1 año', '2': '1 a 4 años', '3': '5 a 9 años', '4': '10 a 14 años',
    '5': '15 a 19 años', '6': '20 a 24 años', '7': '25 a 29 años', '8': '30 a 34 años',
    '9': '35 a 39 años', '10': '40 a 44 años', '11': '45 a 49 años', '12': '50 a 54 años',
    '13': '55 a 59 años', '14': '60 a 64 años', '15': '65 años a más'
}

gender = {
    "1": 'Masculino', "2": 'Femenino', 1: 'Masculino', 2: 'Femenino'
}

month_mapping = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

grupo_enfermedad_mapping = {
    ('A00', 'B99'): 'Ciertas enfermedades infecciosas y parasitarias',
    ('C00', 'D48'): 'Neoplasias',
    ('D50', 'D89'): 'Enfermedades de la sangre y de los órganos hematopoyéticos y otros trastornos que afectan el mecanismo de la inmunidad',
    ('E00', 'E90'): 'Enfermedades endocrinas, nutricionales y metabólicas',
    ('F00', 'F99'): 'Trastornos mentales y del comportamiento',
    ('G00', 'G99'): 'Enfermedades del sistema nervioso',
    ('H00', 'H59'): 'Enfermedades del ojo y sus anexos',
    ('H60', 'H95'): 'Enfermedades del oído y de la apófisis mastoides',
    ('I00', 'I99'): 'Enfermedades del aparato circulatorio',
    ('J00', 'J99'): 'Enfermedades del aparato respiratorio',
    ('K00', 'K93'): 'Enfermedades del aparato digestivo',
    ('L00', 'L99'): 'Enfermedades de la piel y el tejido subcutáneo',
    ('M00', 'M99'): 'Enfermedades del sistema osteomuscular y del tejido conectivo',
    ('N00', 'N99'): 'Enfermedades del aparato genitourinario',
    ('O00', 'O99'): 'Embarazo, parto y puerperio',
    ('P00', 'P96'): 'Ciertas afecciones originadas en el periodo perinatal',
    ('Q00', 'Q99'): 'Malformaciones congénitas, deformidades y anomalías cromosómicas',
    ('R00', 'R99'): 'Síntomas, signos y hallazgos anormales clínicos y de laboratorio, no clasificados en otra parte',
    ('S00', 'T98'): 'Traumatismos, envenenamientos y algunas otras consecuencias de causa externa',
    ('V01', 'Y98'): 'Causas externas de morbilidad y de mortalidad',
    ('Z00', 'Z99'): 'Factores que influyen en el estado de salud y contacto con los servicios de salud',
    ('U00', 'U99'): 'Códigos para situaciones especiales'
}

def get_grupo_enfermedad(diagnostico):
    if diagnostico is None:
        return 'Desconocido'
    code = diagnostico[:3]
    for (start, end), group in grupo_enfermedad_mapping.items():
        if start <= code <= end:
            return group
    return 'Desconocido'

# Register UDFs for mappings
spark.udf.register("get_grupo_enfermedad", get_grupo_enfermedad, StringType())

# Apply mappings and transformations
df = df.withColumn("NU_TOTAL_ATENDIDOS", col("NU_TOTAL_ATENDIDOS").cast("int").na.fill(0)) \
    .withColumn("EDAD", col("EDAD").cast(StringType())) \
    .withColumn("EDAD", when(col("EDAD").isNotNull(), col("EDAD")).map(age_mapping).otherwise(col("EDAD"))) \
    .withColumn("SEXO", col("SEXO").cast(StringType())) \
    .withColumn("SEXO", when(col("SEXO").isNotNull(), col("SEXO")).map(gender).otherwise(col("SEXO"))) \
    .withColumn("MES_NOMBRE", col("MES").cast("int").map(month_mapping)) \
    .withColumn("MES_ANIO", concat_ws("-", col("ANHO").cast("string"), col("MES_NOMBRE"))) \
    .withColumn("NOMBRECIE10", col("DIAGNOSTICO").map(cat_diseases_dict)) \
    .withColumn("Pais", lit("Peru")) \
    .withColumn("GrupoEnfermedad", expr("get_grupo_enfermedad(DIAGNOSTICO)"))

# Save the processed data to a new CSV file
df.write.csv(os.path.join(save_directory, 'df_ambulatorio_processed.csv'), header=True, mode='overwrite')

# Split the data into df_mental and df_fisico
df_mental = df.filter(df.DIAGNOSTICO.startswith('F'))
df_fisico = df.filter(~df.DIAGNOSTICO.startswith('F'))

# Save the final DataFrames to CSV files
df_mental.write.csv(os.path.join(save_directory, 'df_mental.csv'), header=True, mode='overwrite')
df_fisico.write.csv(os.path.join(save_directory, 'df_fisico.csv'), header=True, mode='overwrite')

print("Files saved successfully.")
