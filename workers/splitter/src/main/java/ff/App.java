package ff;

import java.util.ArrayList;
import java.util.Properties;

//import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        Logger logger = LogManager.getLogger(App.class);
        Properties props = new Properties();
        // final Serde<String> stringSerde = Serdes.String();
        // final Serde<Long> longSerde = Serdes.Long();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "splitter-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.124.0.4:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test")
       // .peek((k, v) -> logger.info("Obeserve event: {}", v))
                .flatMapValues((v) -> {
                    ObjectMapper m = new ObjectMapper();
                    JsonNode i;
                    ArrayList<String> a = new ArrayList<String>();
                    try {
                        logger.info("Trying to parse json");
                        i = m.readTree(v.toString());
                        logger.info("JSON parsed");
                        JsonNode x = i.get("items");
                        logger.info("Trasversing to items");
                        //if (!x.isNull()) {
                        if (i.findValue("items") != null)
                        {
                            logger.info("Items is an array");
                            for (JsonNode vv : x)
                            {
                                a.add(vv.toString());
                            }
                        return a;
                        } //end if
                        a.add(v.toString());
                        return a;
                    } catch (Exception e) {
                        logger.debug(e);
                    }
                    return null;
                })
                // .peek( (k,v) -> System.out.println("observe event: " + v.toString()))
                .to("test3");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("Streams started");
    }
}
