package com.hp.gmall.realtime.utils

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingKafkaUtils {
    var kafkaParams= collection.mutable.Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.79.101:9092,192.168.79.102:9092,192.168.79.103:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "default",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

    def getDStream(ssc: StreamingContext, topicSet: Array[String],groupId:String) = {
        kafkaParams("ConsumerConfig.GROUP_ID_CONFIG")=groupId
        KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))
    }

    def getDStream(ssc: StreamingContext, topicSet: Array[String],groupId:String,offsets:Map[TopicPartition,Long]) = {
        kafkaParams("ConsumerConfig.GROUP_ID_CONFIG")=groupId
        KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams,offsets))
    }


}
