package com.hp.gmall.realtime.utils

import cn.hutool.db.nosql.redis.RedisDS
import cn.hutool.setting.Setting
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._

object OffsetManagerUtils {
    val redisSetting="redis.setting"
    def getFromRedis(groupId:String,topic:String)= {

        val jedis: Jedis = RedisDS.create(new Setting(redisSetting), null).getJedis
        val offsetsMap = jedis.hgetAll(groupId + "->" + topic).asScala
        jedis.close()
        if (offsetsMap.isEmpty) {
            null
        } else{
            offsetsMap.map({
                case (partition, offsets) => new TopicPartition(topic,partition.toInt)->offsets.toLong
            }).toMap
        }
    }

    def saveToRedis(groupId:String,offsets:Map[TopicPartition,Long])={
        val jedis: Jedis = RedisDS.create(new Setting(redisSetting), null).getJedis
        offsets.foreach({
            case (topicPartition, offset) =>
                val partition = topicPartition.partition().toString
                val topic = topicPartition.topic()
                jedis.hset(groupId+"->"+topic,Map(partition->offset.toString).asJava)
        })
        jedis.close()
    }
}
