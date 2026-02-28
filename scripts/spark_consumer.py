from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, window, sum as _sum, first, to_timestamp
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StandardScalerModel, MinMaxScalerModel, PCAModel, VectorAssembler
import os

spark = SparkSession.builder \
    .appName("Swasthya-Matrix-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load Models
scaler_model = StandardScalerModel.load("models/swasthya_scaler.model")
kmeans_model = KMeansModel.load("models/swasthya_kmeans.model")
rf_model = RandomForestClassificationModel.load("models/swasthya_rf.model")
pca_model = PCAModel.load("models/swasthya_pca_vulnerability.model")
score_scaler_model = MinMaxScalerModel.load("models/swasthya_score_scaler.model")

# District removed from Schema
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Region_Index", DoubleType(), True),
    StructField("ANC_Registered", DoubleType(), True),
    StructField("Institutional_Delivery", DoubleType(), True),
    StructField("C_Section", DoubleType(), True),
    StructField("Child_Diarrhoea", DoubleType(), True),
    StructField("Child_TB", DoubleType(), True),
    StructField("Immunization_MR", DoubleType(), True),
    StructField("Low_Birth_Weight", DoubleType(), True),
    StructField("Infant_Death_Sepsis", DoubleType(), True),
    StructField("Maternal_Death_Bleeding", DoubleType(), True),
    StructField("Adult_Death_TB", DoubleType(), True),
    StructField("Adult_Suicide", DoubleType(), True),
    StructField("PW_Hypertension", DoubleType(), True),
    StructField("Child_Malaria", DoubleType(), True),
    StructField("Severe_Anaemia_Treated", DoubleType(), True),
    StructField("Condoms_Distributed", DoubleType(), True),
    StructField("Early_Breastfeeding", DoubleType(), True)
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health-stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_stream = parsed_stream.withColumn("timestamp", to_timestamp(col("Date"), "yyyy-MM-dd"))

# District removed from Windowing GroupBy
aggregated_stream = parsed_stream.withWatermark("timestamp", "2 days") \
    .groupBy(
        window(col("timestamp"), "30 days"),
        col("State")
    ).agg(
        first("Region_Index").alias("Region_Index"),
        _sum("ANC_Registered").alias("ANC_Registered"),
        _sum("Institutional_Delivery").alias("Institutional_Delivery"),
        _sum("C_Section").alias("C_Section"),
        _sum("Child_Diarrhoea").alias("Child_Diarrhoea"),
        _sum("Child_TB").alias("Child_TB"),
        _sum("Immunization_MR").alias("Immunization_MR"),
        _sum("Low_Birth_Weight").alias("Low_Birth_Weight"),
        _sum("Infant_Death_Sepsis").alias("Infant_Death_Sepsis"),
        _sum("Maternal_Death_Bleeding").alias("Maternal_Death_Bleeding"),
        _sum("Adult_Death_TB").alias("Adult_Death_TB"),
        _sum("Adult_Suicide").alias("Adult_Suicide"),
        _sum("PW_Hypertension").alias("PW_Hypertension"),
        _sum("Child_Malaria").alias("Child_Malaria"),
        _sum("Severe_Anaemia_Treated").alias("Severe_Anaemia_Treated"),
        _sum("Condoms_Distributed").alias("Condoms_Distributed"),
        _sum("Early_Breastfeeding").alias("Early_Breastfeeding")
    )

def process_batch(df_batch, batch_id):
    if df_batch.count() == 0:
        return

    feature_cols = ['ANC_Registered', 'Institutional_Delivery', 'C_Section', 'Child_Diarrhoea', 
                    'Child_TB', 'Immunization_MR', 'Low_Birth_Weight', 'Infant_Death_Sepsis', 
                    'Maternal_Death_Bleeding', 'Adult_Death_TB', 'Adult_Suicide', 'PW_Hypertension', 
                    'Child_Malaria', 'Severe_Anaemia_Treated', 'Condoms_Distributed', 'Early_Breastfeeding',
                    'Region_Index']
    
    assembler_all = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
    df_assembled = assembler_all.transform(df_batch)
    
    df_scaled = scaler_model.transform(df_assembled)
    
    df_kmeans = kmeans_model.transform(df_scaled)
    df_rf_input = df_kmeans.withColumnRenamed("prediction", "label")
    
    df_rf = rf_model.transform(df_rf_input)
    df_rf = df_rf.withColumnRenamed("prediction", "RF_Severity_Prediction")
    
    negative_features = ['Child_Diarrhoea', 'Child_TB', 'Infant_Death_Sepsis', 'Maternal_Death_Bleeding', 
                         'Adult_Death_TB', 'Adult_Suicide', 'Child_Malaria', 'Severe_Anaemia_Treated']
    
    pca_assembler = VectorAssembler(inputCols=negative_features, outputCol="negative_features_vec", handleInvalid="skip")
    df_pca_assembled = pca_assembler.transform(df_rf)
    
    df_pca_result = pca_model.transform(df_pca_assembled)
    
    from pyspark.ml.functions import vector_to_array
    df_pca_extracted = df_pca_result.withColumn("raw_score_scalar", vector_to_array("raw_pca_score")[0])
    
    score_assembler = VectorAssembler(inputCols=["raw_score_scalar"], outputCol="raw_score_vec", handleInvalid="skip")
    df_score_assembled = score_assembler.transform(df_pca_extracted)
    
    df_final_score = score_scaler_model.transform(df_score_assembled)
    df_final_output = df_final_score.withColumn("Vulnerability_Score", vector_to_array("final_vulnerability_vec")[0])
    
    final_ui_df = df_final_output.select(
        "window.end", "State", 
        col("label").alias("KMeans_Cluster"), 
        "RF_Severity_Prediction", 
        "Vulnerability_Score"
    )
    
    os.makedirs("output_sink", exist_ok=True)
    final_ui_df.toPandas().to_csv(f"output_sink/batch_{batch_id}.csv", index=False)

query = aggregated_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()