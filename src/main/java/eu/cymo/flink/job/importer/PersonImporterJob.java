package eu.cymo.flink.job.importer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import eu.cymo.flink.PropertiesLoader;
import eu.cymo.flink.job.Topics;
import eu.cymo.flink.person.AnalyticsPerson;
import eu.cymo.flink.person.CmsPerson;
import eu.cymo.flink.person.Person;

public class PersonImporterJob {
    
    public static void main(String[] args) throws Exception {
        var env  = StreamExecutionEnvironment.getExecutionEnvironment();
        
        var cmsSource = KafkaSource.<CmsPerson>builder()
                .setProperties(PropertiesLoader.loadKafkaProperties())
                .setTopics(Topics.TOPIC_CMS_PERSON)
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        
        var analyticsSource = KafkaSource.<AnalyticsPerson>builder()
                .setProperties(PropertiesLoader.loadKafkaProperties())
                .setTopics(Topics.TOPIC_ANALYTICS_PERSON)
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        
        var serializer = KafkaRecordSerializationSchema.<Person>builder()
                .setTopic(Topics.TOPIC_PERSON)
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build();
        
        var sink = KafkaSink.<Person>builder()
                .setKafkaProducerConfig(PropertiesLoader.loadKafkaProperties())
                .setRecordSerializer(serializer)
                .build();
        
        var cmsStream = env.fromSource(
                cmsSource,
                WatermarkStrategy.noWatermarks(),
                "cms_person_source");
        
        var analyticsStream = env.fromSource(
                analyticsSource,
                WatermarkStrategy.noWatermarks(),
                "analytis_person_source");
        
        defineWorkflow(cmsStream, analyticsStream)
            .sinkTo(sink);
        
        env.execute("person_importer");
    }
    
    public static DataStream<Person> defineWorkflow(
            DataStream<CmsPerson> cmsPerson,
            DataStream<AnalyticsPerson> analtyicsPerson) {
        
        return cmsPerson.connect(analtyicsPerson)
                .map(new CoMapFunction<CmsPerson, AnalyticsPerson, Person>() {

                    @Override
                    public Person map1(CmsPerson value) throws Exception {
                        return value.toPerson();
                    }

                    @Override
                    public Person map2(AnalyticsPerson value) throws Exception {
                        return value.toPerson();
                    }
                })
                .filter(PersonImporterJob::isAnAdult);
    }
    
    private static boolean isAnAdult(Person person) {
        return person.getAge() >= 18;
    }
    
    

}
