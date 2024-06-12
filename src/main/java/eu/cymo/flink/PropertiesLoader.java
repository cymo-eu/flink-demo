package eu.cymo.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;


public class PropertiesLoader {
    public static final String KAFKA_PROPERTIES = "kafka.properties";
    
    public static Properties loadClasspathFile(String file) throws IOException, ConfigurationException {
        var config = new PropertiesConfiguration();
        var handler = new FileHandler(config);
        var properties = new Properties();
        try(var stream = PropertiesLoader.class.getClassLoader().getResourceAsStream(file)) {
            handler.load(stream);
            var keys = config.getKeys();
            while(keys.hasNext()) {
                var key = keys.next();
                properties.put(key, config.getString(key));
            }
            return properties;
        }
    }
    
    public static Properties loadKafkaProperties() throws IOException, ConfigurationException {
        return loadClasspathFile(KAFKA_PROPERTIES);
    }
}
