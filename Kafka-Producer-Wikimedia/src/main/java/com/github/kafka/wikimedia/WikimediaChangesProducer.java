package com.github.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
//docker run -d -p 80:80 docker/getting-started
//  curl -sS https://raw.githubusercontent.com/conduktor/conduktor-platform/main/example-local/autorun/autorun.sh | sh -s setup
public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "Wikimedia_Changes";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //adding best practices
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20000");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,"45000");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"120000");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(1024*1024));

        //create event handler to handle events from the stream and push to producer

        final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        EventHandler eventHandler = new EventHandler() {
            @Override
            public void onOpen() throws Exception {

            }

            @Override
            public void onClosed() throws Exception {
                kafkaProducer.flush();
            }

            @Override
            public void onMessage(String event, MessageEvent messageEvent) throws Exception {
                //asyn code
                log.info(messageEvent.getData());
               List<RecordHeader> headers = new ArrayList<>();
               headers.add(new RecordHeader("domain", messageEvent.getOrigin().getPath().getBytes()));
                kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {

                       if(exception !=null) log.error(exception.toString());
                       log.info(String.valueOf(metadata.offset()));


                    }
                });

            }

            @Override
            public void onComment(String comment) throws Exception {

            }

            @Override
            public void onError(Throwable t) {
                log.error("Error in streaming!!");

            }
        };

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        //we  need to pass this URL to event source
        EventSource.Builder eventBuilder = new EventSource.Builder(eventHandler, URI.create(url));
        // later need to build event source from builder
        EventSource eventSource = eventBuilder.build();

        //start producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
