package com.hp.gmall.realtime.utils

import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._

object OffsetManagerUtils {

  def getFromRedis(groupId: String, topic: String): Map[TopicPartition, Long] = {

    val jedis = RedisUtils.create().getJedis
    val offsetsMap = jedis.hgetAll(groupId + "->" + topic).asScala
    jedis.close()
    if (offsetsMap.isEmpty) {
      null
    } else {
      offsetsMap.map({
        case (partition, offsets) => new TopicPartition(topic, partition.toInt) -> offsets.toLong
      }).toMap
    }
  }

  def saveToRedis(groupId: String, offsets: Map[TopicPartition, Long]): Unit = {
    val jedis = RedisUtils.create().getJedis
    offsets.foreach({
      case (topicPartition, offset) =>
        val partition = topicPartition.partition().toString
        val topic = topicPartition.topic()
        jedis.hset(groupId + "->" + topic, Map(partition -> offset.toString).asJava)
    })
    jedis.close()
  }
}
