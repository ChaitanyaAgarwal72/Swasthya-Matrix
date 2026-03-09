from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, window, sum as _sum, to_timestamp, lit
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StandardScalerModel, MinMaxScalerModel, PCAModel, VectorAssembler
import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName("Swasthya-Matrix-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

scaler_model = StandardScalerModel.load("models/swasthya_kmeans_scaler.model")
rf_model = RandomForestClassificationModel.load("models/swasthya_rf.model")
pca_scaler_model = StandardScalerModel.load("models/swasthya_pca_scaler.model")
pca_model = PCAModel.load("models/swasthya_pca_vulnerability.model")
score_scaler_model = MinMaxScalerModel.load("models/swasthya_score_scaler.model")

print("All models loaded successfully.")

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

parsed_stream = parsed_stream.withColumn("timestamp", to_timestamp(col("Date"), "dd-MM-yyyy"))

aggregated_stream = parsed_stream.withWatermark("timestamp", "2 days") \
    .groupBy(
        window(col("timestamp"), "30 days"),
        col("State"),
        col("Region_Index")
    ).agg(
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
    try:
        row_count = df_batch.count()
        print(f"Batch {batch_id}: received {row_count} rows.")
        if row_count == 0:
            print(f"Batch {batch_id}: empty, skipping.")
            return

        feature_cols = ['ANC_Registered', 'Institutional_Delivery', 'C_Section', 'Child_Diarrhoea', 
                        'Child_TB', 'Immunization_MR', 'Low_Birth_Weight', 'Infant_Death_Sepsis', 
                        'Maternal_Death_Bleeding', 'Adult_Death_TB', 'Adult_Suicide', 'PW_Hypertension', 
                        'Child_Malaria', 'Severe_Anaemia_Treated', 'Condoms_Distributed', 'Early_Breastfeeding',
                        'Region_Index']
        
        assembler_all = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
        df_assembled = assembler_all.transform(df_batch)
        df_scaled = scaler_model.transform(df_assembled)
        
        df_rf = rf_model.transform(df_scaled)
        df_rf = df_rf.withColumnRenamed("prediction", "Severity_Level")
        
        negative_features = ['Child_Diarrhoea', 'Child_TB', 'Infant_Death_Sepsis', 'Maternal_Death_Bleeding', 
                             'Adult_Death_TB', 'Adult_Suicide', 'Child_Malaria', 'Severe_Anaemia_Treated']
        pca_assembler = VectorAssembler(inputCols=negative_features, outputCol="negative_features_vec", handleInvalid="skip")
        df_pca_assembled = pca_assembler.transform(df_rf)
        df_pca_scaled = pca_scaler_model.transform(df_pca_assembled)
        df_pca_result = pca_model.transform(df_pca_scaled)
        
        from pyspark.ml.functions import vector_to_array
        df_pca_extracted = df_pca_result.withColumn("raw_score_scalar", vector_to_array("raw_pca_score")[0])
        score_assembler = VectorAssembler(inputCols=["raw_score_scalar"], outputCol="raw_score_vec", handleInvalid="skip")
        df_score_assembled = score_assembler.transform(df_pca_extracted)
        df_final_score = score_scaler_model.transform(df_score_assembled)
        df_final_output = df_final_score.withColumn("Vulnerability_Score", lit(101.0) - vector_to_array("final_vulnerability_vec")[0])
        
        pandas_df = df_final_output.select(
            col("window.end"), col("State"), col("Region_Index"),
            col("Severity_Level"),
            col("Vulnerability_Score")
        ).toPandas()

        pandas_df['Region'] = pandas_df['Region_Index'].map({0.0: 'Rural', 1.0: 'Urban'})
        pandas_df['Location'] = pandas_df['State'] + " (" + pandas_df['Region'] + ")"
        pandas_df = pandas_df.rename(columns={'end': 'Month_Ending'})

        pandas_df = pandas_df.sort_values('Month_Ending').groupby(['State', 'Region_Index']).last().reset_index()

        os.makedirs("output_sink", exist_ok=True)
        pandas_df.to_csv("output_sink/latest_predictions.csv", index=False)
        print(f"Batch {batch_id}: CSV written with {len(pandas_df)} rows.")

        try:
            hdfs_path = f"hdfs://localhost:9000/swasthya_data/predictions/batch_{batch_id}"
            df_final_output.select(
                col("window.end"), col("State"), col("Region_Index"),
                col("Severity_Level"), col("Vulnerability_Score")
            ).write.mode("overwrite").csv(hdfs_path, header=True)
            print(f"Batch {batch_id}: also written to HDFS.")
        except Exception:
            pass

    except Exception as e:
        print(f"ERROR in process_batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

query = aggregated_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

os.makedirs("output_sink", exist_ok=True)
with open("output_sink/.spark_ready", "w") as f:
    f.write("ready")
print("Spark streaming query started. Producer can now send data.")

query.awaitTermination()