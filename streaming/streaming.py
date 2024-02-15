from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, StringType, TimestampType, IntegerType, DoubleType, StructField, StructType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
from cassandra.cluster import Cluster


keyspace = "bigdata"
pollution_table = "pollution"
traffic_table = "traffic"

def writePollutionToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=pollution_table, keyspace=keyspace) \
        .save()
    print("Data written to Cassandra for pollution table")

def writeTrafficToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=traffic_table, keyspace=keyspace) \
        .save()
    print("Data written to Cassandra for traffic table")
    
def create_database(cassandra_session):

    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("DROP TABLE IF EXISTS bigdata.pollution")
    cassandra_session.execute("""
        CREATE TABLE bigdata.pollution (
            date TIMESTAMP PRIMARY KEY,
            laneId text,
            laneCO double,
            laneCO2 double,
            laneHC double,
            laneNOx double,
            lanePMx double,
            laneNoise double
        )
    """)
    
    cassandra_session.execute("DROP TABLE IF EXISTS bigdata.traffic")
    cassandra_session.execute("""
        CREATE TABLE bigdata.traffic (
            date TIMESTAMP PRIMARY KEY,
            laneId text,
            vehicleCount int
        )
    """)


if __name__ == "__main__":

    cassandra_host = os.getenv('CASSANDRA_HOST')
    cassandra_port = int(os.getenv('CASSANDRA_PORT'))
    kafka_url = os.getenv('KAFKA_URL')
    #topic = os.getenv('KAFKA_TOPIC')
    fcd_topic = 'stockholm-fcd'
    emission_topic = 'stockholm-emission'
    
    window_duration = os.getenv('WINDOW_DURATION')
    N = int(os.getenv('N'))

    cassandra_cluster = Cluster([cassandra_host], port=cassandra_port)
    cassandra_session = cassandra_cluster.connect()
    create_database(cassandra_session)

    vehicleSchema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_id", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_slope", FloatType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    emissionSchema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_CO", FloatType()),
        StructField("vehicle_CO2", FloatType()),
        StructField("vehicle_HC", FloatType()),
        StructField("vehicle_NOx", FloatType()),
        StructField("vehicle_PMx", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_eclass", StringType()),
        StructField("vehicle_electricity", FloatType()),
        StructField("vehicle_id", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_fuel", FloatType()),
        StructField("vehicle_noise", FloatType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_route", StringType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_waiting", FloatType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])


    appName = "StockholmApp"
    
    conf = SparkConf()
    conf.set("spark.cassandra.connection.host", cassandra_host)
    conf.set("spark.cassandra.connection.port", cassandra_port)
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    dfEmission = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", emission_topic) \
        .load()
    
    dfEmission.printSchema()
    
    dfFcd = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", fcd_topic) \
        .load()
    
    dfFcd.printSchema()

    #### TASK CODE STARTS HERE

    dfEmissionParsed = dfEmission.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), emissionSchema).alias("data")).select("data.*")
    dfEmissionParsed = dfEmissionParsed.withColumn("timestep_time", to_timestamp(dfEmissionParsed['timestep_time'])) 
    dfEmissionParsed.printSchema()

    dfFcdParsed = dfFcd.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), vehicleSchema).alias("data")).select("data.*")
    dfFcdParsed = dfFcdParsed.withColumn("timestep_time", to_timestamp(dfFcdParsed['timestep_time'])) 
    dfFcdParsed.printSchema()


    # Task a - Pollution Zones
    grouped_data_pollution = dfEmissionParsed.groupBy(window("timestep_time", window_duration).alias("date"), col("vehicle_lane").alias("laneId")) \
    .agg(
        avg("vehicle_CO").alias("laneCO"),
        avg("vehicle_CO2").alias("laneCO2"),
        avg("vehicle_HC").alias("laneHC"),
        avg("vehicle_NOx").alias("laneNOx"),
        avg("vehicle_PMx").alias("lanePMx"),
        avg("vehicle_noise").alias("laneNoise")
    )#.drop("window")

    # print("PRINTING POLLUTION DATA")
    # grouped_data_pollution.show(50)  # Prikazuje prvih 50 redova

    # grouped_data_pollution = grouped_data_pollution.orderBy("laneCO", "laneCO2", "laneHC", "laneNOx", "lanePMx", "laneNoise", ascending=[False, False, False, False, False, False])
    
    # grouped_data_pollution = grouped_data_pollution.withColumn("sum_emissions", sum("laneCO", "laneCO2", "laneHC", "laneNOx", "lanePMx", "laneNoise"))
    # grouped_data_pollution = grouped_data_pollution.orderBy("sum_emissions", ascending=False)
    # grouped_data_pollution = grouped_data_pollution.drop("sum_emissions")


    # Task b - Traffic Streets
    grouped_data_traffic = dfFcdParsed.groupBy(window("timestep_time", window_duration).alias("date"),
        col("vehicle_lane").alias("laneId")).agg(approx_count_distinct("vehicle_id").alias("vehicleCount"))#.drop("window")
    
    # print("PRINTING POLLUTION DATA")
    # query_pollution2 = grouped_data_pollution.writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .option("truncate", "false") \
    # .start()

    # query_pollution2.awaitTermination()

    # print("PRINTING TRAFFIC DATA")
    # query_traffic2 = grouped_data_traffic.writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .option("truncate", "false") \
    # .start()

    # query_traffic2.awaitTermination()


    # query_traffic = grouped_data_traffic.writeStream \
    #     .outputMode("complete") \
    #     .foreachBatch(writeTrafficToCassandra) \
    #     .start()

    query_traffic = grouped_data_traffic.writeStream \
        .option("spark.cassandra.connection.host", "cassandra:9042") \
        .foreachBatch(writeTrafficToCassandra) \
        .outputMode("update") \
        .start() 
    
    query_traffic.awaitTermination()
    
    # query_pollution = grouped_data_pollution.writeStream \
    #     .outputMode("complete") \
    #     .foreachBatch(writePollutionToCassandra) \
    #     .start()

    query_pollution = grouped_data_pollution.writeStream \
        .option("spark.cassandra.connection.host", "cassandra:9042") \
        .foreachBatch(writePollutionToCassandra) \
        .outputMode("update") \
        .start() 

    query_pollution.awaitTermination()


    spark.stop()

