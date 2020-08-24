package com.hp.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.io.*;
import java.util.Enumeration;
import java.util.Properties;


public class RedisUtils implements Closeable {

    public final static String REDIS_CONFIG_PATH = "conf/redis.properties";
    private JedisPool pool;

    public RedisUtils(String confPath) {
        this.init(confPath);
    }

    public static RedisUtils create() {
        return new RedisUtils(REDIS_CONFIG_PATH);
    }

    public static RedisUtils create(String confPath) {
        return new RedisUtils(confPath);
    }

    public Jedis getJedis() {
        return this.pool.getResource();
    }

    public RedisUtils init(String confPath){
        final JedisPoolConfig config = new JedisPoolConfig();
        Properties prop = null;
        InputStream inputStream = null;
        try {
            prop = new Properties();
            inputStream = new FileInputStream(confPath);
            prop.load(inputStream);
            String host = prop.getProperty("host", Protocol.DEFAULT_HOST);
            int port = Integer.parseInt(prop.getProperty("port", String.valueOf(Protocol.DEFAULT_PORT)));
            int connectionTimeout = Integer.parseInt(prop.getProperty("connectionTimeout", String.valueOf(Protocol.DEFAULT_TIMEOUT)));
            int soTimeout = Integer.parseInt(prop.getProperty("soTimeout", String.valueOf(Protocol.DEFAULT_TIMEOUT)));
            String password = prop.getProperty("password", null);
            int database = Integer.parseInt(prop.getProperty("database", String.valueOf(Protocol.DEFAULT_DATABASE)));
            String clientName = prop.getProperty("clientName", this.getClass().getName());
            boolean ssl = Boolean.parseBoolean(prop.getProperty("ssl", "false"));

            this.pool = new JedisPool(config, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl, null, null, null
            );
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream !=null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return this;
    }


    public void close() {
        if (null != pool) {
            try {
                pool.close();
            } catch (Exception e) {
                // 静默关闭
                e.printStackTrace();
            }
        }
    }
}
