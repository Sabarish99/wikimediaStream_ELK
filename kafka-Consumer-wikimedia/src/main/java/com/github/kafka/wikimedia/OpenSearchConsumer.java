package com.github.kafka.wikimedia;

import com.google.gson.JsonParser;
import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.*;

public class OpenSearchConsumer {

    static final Logger logger =  LoggerFactory.getLogger(OpenSearchConsumer.class);
// first - Need to create opensearch client
        public static RestHighLevelClient createOpenSearchClient()
        {
            RestHighLevelClient restHighLevelClient;
            String bonsaiURL = "https://srab45ulq6:og1j593i8a@opensearch-wikimedia-3188383116.us-east-1.bonsaisearch.net:443";
            URI connectionURL = URI.create(bonsaiURL);

            String userInfo = connectionURL.getUserInfo();
            String rawInfo = connectionURL.getRawUserInfo();
            logger.info("UserInfo "+ userInfo);





            //make secure / unsecure connection based on userInfo (generally if created from Docker, it falls with userInfo = null
            //client with no security
            if(userInfo == null) restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURL.getHost(),connectionURL.getPort(),"http")));

            else {
                logger.info("Executing with security");
                String[] auth = userInfo.split(":");
                logger.info(auth[0]);
                logger.info(auth[1]);

                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0],auth[1]));

                restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURL.getHost(),connectionURL.getPort(),connectionURL.getScheme()))
                        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

                //in the above line, we create restClient with builder using host,port and schema and providining the credentials info and setting up keepAlive Stratergy for that client request using
                //call back technique. Lambda expressions are used in setHttpClientConfigCallback()
            }

            return  restHighLevelClient;
        }


        public static KafkaConsumer<String, String> createKafkaClient()
        {
            String bootstrapServer = "127.0.0.1:9092";


            String group_id = "Elastic_Consumer";

            //create Kafka client
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,group_id+"_instance");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");


            return new KafkaConsumer<String, String>(properties);
        }

        public static String extractID(String json){
            return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
        }

    public static void main(String[] args) throws IOException {

        String topic = "Wikimedia_Changes";
        String index = "wikimedia_opensearch";


            RestHighLevelClient openSearchClient = createOpenSearchClient();
            //need to create index on opensearch for storing data if it doesn't exsists

            //check if indices exisits in opensearch\
        try{
            boolean isIndicesExsists = openSearchClient.indices().exists(new GetIndexRequest(index),RequestOptions.DEFAULT);

            if(!isIndicesExsists) {

                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);

                //adding the index created to opensearch
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Wikimedia Index created");
            }
            else {
                logger.info("Index already exists");
            }



        } catch (IOException e) {
//           logger.error(e.getCause().toString());
            new Throwable(e.getCause());
        }

        KafkaConsumer<String, String> consumer = createKafkaClient();
        ConsumerRebalanceListenerImpl rebalanceListener = new ConsumerRebalanceListenerImpl(consumer);


        //get reference of current thread
//        final  Thread thread = Thread.currentThread();
//        logger.info(thread.toString());
//        //adding ShutDownHook
//        Runtime.getRuntime().addShutdownHook(new Thread(
//                () -> {
//            logger.info("SHUTDOWN detected!! calling consumer WakeUP");
//            consumer.wakeup();
//
//            //join main thread to allow execution of code in main
//            try {
//                thread.join();
//            } catch (Exception e) {
//               e.printStackTrace();
//            }
//        }));

        consumer.assign(Collections.singleton(new TopicPartition(topic,4))) ;
        try {

            consumer.subscribe(Arrays.asList(topic));


            while (true) {

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                int numberOfRecords = consumerRecords.count();
                logger.info("Number of records polled " + numberOfRecords); //it should be equal to maxPollRecords if there are more data


                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    //sendng records 1 by 1 to Opensearch

                   try{
                       String id = extractID(consumerRecord.value());
                       IndexRequest indexRequest = new IndexRequest(index)
                               .source(consumerRecord.value(), XContentType.JSON).id(id);

                       //logger.info("Index request " + indexRequest);
                       IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                       logger.info("RESPONSE " + response.getId());
                       consumer.commitAsync();
                   }
                   catch (Exception e){
                        logger.error(e.getCause().toString());
                   }

                    //above technique is not efficient as it pushes 1 record only to Opensearch
//                    rebalanceListener.addOffsetsToCommit(topic, consumerRecord.partition(), consumerRecord.offset());

                }


            }
//        }catch (WakeupException e) {
//            logger.info("Wakeup Exception");
//        }

        }catch (Exception e) {
            logger.error(e.toString());
        }

//        finally{
//            try{
//                consumer.commitSync(rebalanceListener.getOffsets());
//            }
//            finally {
//                consumer.close();
//                logger.info("GRACEFUL SHUTDOWN");
//            }
//        }

    }
}
