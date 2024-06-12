package eu.cymo.flink.job.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import eu.cymo.flink.PropertiesLoader;
import eu.cymo.flink.job.Topics;
import eu.cymo.flink.person.AnalyticsPerson;
import eu.cymo.flink.person.CmsPerson;

public class DataGenJob {
    
    public static void main(String[] args) throws Exception {
        var env  = StreamExecutionEnvironment.getExecutionEnvironment();
        
        cmsPersonFlow(env);
        analyticsPersonFlow(env);
        
        env.execute("data_gen_job");
    }
    
    private static void cmsPersonFlow(StreamExecutionEnvironment env) throws Exception {
        var source = new DataGeneratorSource<>(
                i -> CmsPersonGenerator.generate(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.POJO(CmsPerson.class));
        
        var serializer = KafkaRecordSerializationSchema.<CmsPerson>builder()
                .setTopic(Topics.TOPIC_CMS_PERSON)
                .setValueSerializationSchema(new JsonSerializationSchema<>(DataGenJob::getMapper))
                .build();

        var sink = KafkaSink.<CmsPerson>builder()
                .setKafkaProducerConfig(PropertiesLoader.loadKafkaProperties())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "cms_person_source")
            .sinkTo(sink)
            .name("cms_person_sink");
    }
    
    private static void analyticsPersonFlow(StreamExecutionEnvironment env) throws Exception {
        var source = new DataGeneratorSource<>(
                i -> AnalyticsPersonGenerator.generate(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.POJO(AnalyticsPerson.class));
        
        var serializer = KafkaRecordSerializationSchema.<AnalyticsPerson>builder()
                .setTopic(Topics.TOPIC_ANALYTICS_PERSON)
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build();

        var sink = KafkaSink.<AnalyticsPerson>builder()
                .setKafkaProducerConfig(PropertiesLoader.loadKafkaProperties())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "analytics_person_source")
            .sinkTo(sink)
            .name("analytics_person_sink");
    }
    
    private static ObjectMapper getMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
    
}
