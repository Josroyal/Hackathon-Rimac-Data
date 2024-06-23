import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("RimacHackathon").getOrCreate()

# Define the file names and directory
file_names = [
    'ConsultaB2_2023_I_v1_0.csv',
    'ConsultaB2_2023_II_v1.csv',
    'ConsultaB2_2023_05_v1.csv',
    'ConsultaB2_2023_06_v1.csv',
    'ConsultaB2_2023_07_v1.csv',
    'ConsultaB2_2023_08_v1.csv',
    'ConsultaB2_2023_09_v1.csv',
    'ConsultaB2_2023_10_v1.csv',
    'ConsultaB2_2023_11_v1.csv',
    'ConsultaB2_2023_12_v1.csv',
    'ConsultaB2_2024_01_v1.csv',
    'ConsultaB2_2024_02_v1.csv',
    'ConsultaB2_2024_03_v2.csv',
    'ConsultaB2_2024_04_v1.csv'
]

directory = 's3://disperse/'
save_directory = 's3://acul/'

# Ensure the save directory exists
if not os.path.exists(save_directory):
    os.makedirs(save_directory, exist_ok=True)

# Initialize an empty list to store DataFrames
data_frames = []

# Function to log problematic rows
def log_problematic_rows(file_path, delimiter):
    problematic_rows = []
    expected_num_fields = None
    with open(file_path, encoding='latin-1') as f:
        for i, line in enumerate(f):
            fields = line.split(delimiter)
            if expected_num_fields is None:
                expected_num_fields = len(fields)
            elif len(fields) != expected_num_fields:
                problematic_rows.append((i + 1, line))
    return problematic_rows

# Loop through the filenames, read each file and append the DataFrame to the list
for file_name in file_names:
    file_path = os.path.join(directory, file_name)
    print(f"Reading file: {file_path}")

    # Log problematic rows
    problems = log_problematic_rows(file_path, delimiter=';')
    if problems:
        print(f"Found problems in file {file_name}:")
        for row_number, row_content in problems:
            print(f"Problem in row {row_number}: {row_content.strip()}")

    try:
        df = spark.read.option("delimiter", ";").option("header", "true").option("encoding", "latin-1").csv(file_path)
        data_frames.append(df)
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")

# Concatenate all DataFrames into a single DataFrame
combined_df = data_frames[0]
for df in data_frames[1:]:
    combined_df = combined_df.union(df)

# Save the combined DataFrame to a new CSV file
combined_df.write.option("header", "true").csv(os.path.join(save_directory, 'df_ambulatorio_nasty.csv'))

# Count rows with NaN values
rows_with_nan = combined_df.filter(F.isnan(col("any_column_name"))).count()
print(f"Rows with NaN values: {rows_with_nan}")

# Drop rows with any NaN values
prueba = combined_df.dropna()

# Display the resulting DataFrame
prueba.show()

# Save the cleaned DataFrame to a new CSV file
prueba.write.option("header", "true").csv(os.path.join(save_directory, 'df_ambulatorio_clean.csv'))

print("Files saved successfully.")
