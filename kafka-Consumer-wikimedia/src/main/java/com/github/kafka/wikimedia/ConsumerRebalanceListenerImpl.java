package com.github.kafka.wikimedia;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    Logger log = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);
    private KafkaConsumer<String, String> kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currOffsets = new HashMap<>();

    public ConsumerRebalanceListenerImpl(KafkaConsumer<String,String> kafkaConsumer){
        this.kafkaConsumer = kafkaConsumer;
    }
    public void addOffsetsToCommit(String topic, int topicPartitions , long offset){

        //Topic Partitions takes Strng topic and int partitions as Params
        currOffsets.put( new TopicPartition(topic,topicPartitions) ,
                new OffsetAndMetadata(offset+1,null)
        );
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsets(){
        return currOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Partitions Revoked call back is triggered");
            log.info("Offset to commit "+ currOffsets);
            //doing a sync commit to ensure retries happen if commit is failed
            kafkaConsumer.commitSync(currOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partitions Assignment triggered");

    }
}
