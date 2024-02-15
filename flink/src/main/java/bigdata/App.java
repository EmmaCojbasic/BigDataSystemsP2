package bigdata;

import bigdata.deserializers.*;
import bigdata.pojo.*;
import bigdata.analytics.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.time.Duration;
import java.util.Date;

public class App {
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DeserializationSchema<VehicleInfo> vehicleSchema = new VehicleInfoDeserializer();
        final DeserializationSchema<EmissionInfo> emissionSchema = new EmissionInfoDeserializer();

        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            private static final long serialVersionUID = 1L;
            @Override
            protected Cluster buildCluster(Builder builder) {
                return builder.addContactPoints("cassandra-node").withPort(9042).build();
            }
        };

        KafkaSource<VehicleInfo> vehicleInfoKafkaSource = KafkaSource.<VehicleInfo>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("stockholm-fcd")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(vehicleSchema))
                .build();

        KafkaSource<EmissionInfo> emissionInfoKafkaSource = KafkaSource.<EmissionInfo>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("stockholm-emission")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(emissionSchema))
                .build();

        DataStream<EmissionInfo> emissionInfoDataStream = env.fromSource(emissionInfoKafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "emi-source")
                .filter(new FilterFunction<EmissionInfo>() {
                    @Override
                    public boolean filter(EmissionInfo emissionInfo) throws Exception {
                        return emissionInfo.VehicleLane != null;
                    }
                });

        DataStream<VehicleInfo> vehicleInfoDataStream = env.fromSource(vehicleInfoKafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "veh-source")
                .filter(new FilterFunction<VehicleInfo>() {
                    @Override
                    public boolean filter(VehicleInfo vehicleInfo) throws Exception {
                        return vehicleInfo.VehicleLane != null && vehicleInfo.VehicleId != 0;
                    }
                });


        // --------------------------------------------------------------------------------------------------------------------

        WindowedStream<VehicleInfo, String, TimeWindow> laneGroupedWindowedStream = vehicleInfoDataStream
                .keyBy(VehicleInfo::getVehicleLane)
                .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.seconds(30)));


        WindowedStream<EmissionInfo, String, TimeWindow> laneGroupedWindowedStream2 = emissionInfoDataStream
                .keyBy(EmissionInfo::getVehicleLane)
                .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.seconds(30)));

        //pollution
        DataStream<PollutionData> laneEmissions = LaneAnalyzer.laneAggregation(laneGroupedWindowedStream2);

        CassandraSink.addSink(laneEmissions)
                .setMapperOptions(() -> new Mapper.Option[]{
                        Mapper.Option.saveNullFields(true)
                })
                .setClusterBuilder(clusterBuilder)
                .build();

        //traffic
        DataStream<TrafficData> vehicleCount = LaneAnalyzer.calculateVehiclesOnLane(laneGroupedWindowedStream);

        CassandraSink.addSink(vehicleCount)
                .setMapperOptions(() -> new Mapper.Option[]{
                        Mapper.Option.saveNullFields(true)
                })
                .setClusterBuilder(clusterBuilder)
                .build();


        env.execute("Stockholm");
    }

}
