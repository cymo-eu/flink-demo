package eu.cymo.flink.job.datagen;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import eu.cymo.flink.job.importer.PersonImporterJob;
import eu.cymo.flink.person.AnalyticsPerson;
import eu.cymo.flink.person.CmsPerson;
import eu.cymo.flink.person.Person;

class PersonImporterJobTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(2)
                    .setNumberSlotsPerTaskManager(1)
                    .build());
    
    private StreamExecutionEnvironment env;
    private DataStream.Collector<Person> collector;
    
    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        collector = new DataStream.Collector<>();
    }
    
    @Test
    void mapsAnalyticsPerson_toPerson() throws Exception {
        // given
        var analyticsPerson = new AnalyticsPerson(123, "John Doe", 24);
        
        var cmsStream = cmsStream();
        var analyticsStream = analyticsStream(analyticsPerson);
        
        // when
        PersonImporterJob.defineWorkflow(cmsStream, analyticsStream)
            .collectAsync(collector);
        
        // then
        env.executeAsync();

        var result = collectOutput();
        
        assertThat(result).hasSize(1);
        assertThat(result).contains(new Person("123", "John", "Doe", 24));
    }
    
    @Test
    void ignoresAnalyticsPersonsBelowTheAgeOf18() throws Exception {
        // given
        var analyticsPerson = new AnalyticsPerson(123, "John Doe", 15);
        
        var cmsStream = cmsStream();
        var analyticsStream = analyticsStream(analyticsPerson);
        
        // when
        PersonImporterJob.defineWorkflow(cmsStream, analyticsStream)
            .collectAsync(collector);
        
        // then
        env.executeAsync();

        var result = collectOutput();
        assertThat(result).isEmpty();
    }
    
    @Test
    void mapsCmsPerson_toPerson() throws Exception {
        // given
        var cmsPerson = new CmsPerson("123", "John", "Doe", LocalDate.now().minusYears(24));
        
        var cmsStream = cmsStream(cmsPerson);
        var analyticsStream = analyticsStream();
        
        // when
        PersonImporterJob.defineWorkflow(cmsStream, analyticsStream)
            .collectAsync(collector);
        
        // then
        env.executeAsync();

        var result = collectOutput();
        
        assertThat(result).hasSize(1);
        assertThat(result).contains(new Person("123", "John", "Doe", 24));
    }
    
    @Test
    void ignoresCmsPersonsBelowTheAgeOf18() throws Exception {
        // given
        var cmsPerson = new CmsPerson("123", "John", "Doe", LocalDate.now().minusYears(15));
        
        var cmsStream = cmsStream(cmsPerson);
        var analyticsStream = analyticsStream();
        
        // when
        PersonImporterJob.defineWorkflow(cmsStream, analyticsStream)
            .collectAsync(collector);
        
        // then
        env.executeAsync();

        var result = collectOutput();
        assertThat(result).isEmpty();
    }
    
    private DataStream<CmsPerson> cmsStream(CmsPerson...persons) {
        if(persons.length == 0) {
            return env.addSource(new EmptyCmsPersonSource(), "cms_person");
        }
        return env.fromElements(persons);
    }
    
    private DataStream<AnalyticsPerson> analyticsStream(AnalyticsPerson...persons) {
        if(persons.length == 0) {
            return env.addSource(new EmptyAnalyticsPersonSource(), "analytics_person");
        }
        return env.fromElements(persons);
    }
    
    private List<Person> collectOutput() {
        var persons = new ArrayList<Person>();
        collector.getOutput().forEachRemaining(persons::add);
        return persons;
    }

    public static class EmptyCmsPersonSource extends EmptySource<CmsPerson> {}

    public static class EmptyAnalyticsPersonSource extends EmptySource<AnalyticsPerson> {}
    
    public static class EmptySource<T> implements SourceFunction<T> {

        @Override
        public void run(SourceContext<T> ctx) throws Exception { }

        @Override
        public void cancel() { }
        
    }
}
