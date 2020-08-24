package com.hp.gmall.realtime.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

object StreamUtils {
    var kafkaParams = collection.mutable.Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.79.200:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "default",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

    def getDStream(ssc: StreamingContext, topicSet: Array[String], groupId: String, offsets: Map[TopicPartition, Long]=null): InputDStream[ConsumerRecord[String, String]] = {

        kafkaParams("ConsumerConfig.GROUP_ID_CONFIG") = groupId
        if (offsets==null || offsets.isEmpty) {
            KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))
        } else {
            KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams, offsets))
        }
    }

    def getDStreamWithOffsets(ssc: StreamingContext, topicSet: Array[String], groupId: String, offsets: Map[TopicPartition, Long]): DStream[(ConsumerRecord[String, String], Map[TopicPartition, String])] = {

        val sourceStream = getDStream(ssc, topicSet, groupId, offsets).map(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            //本批次消费到获取偏移量
            val offsets = offsetRanges.map(x => (x.topicPartition(), x.topic)).toMap
            (rdd,offsets)
        })
        sourceStream
    }

}

