import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.io.Closeable;


public class RedisUtil implements Closeable {

    public final static String REDIS_CONFIG_PATH = "config/redis.setting";
    private JedisPool pool;

    public RedisUtil(String confPath) {
        this.init(confPath);
    }

    public static RedisUtil create() {
        return new RedisUtil(REDIS_CONFIG_PATH);
    }

    public static RedisUtil create(String confPath) {
        return new RedisUtil(confPath);
    }

    public Jedis getJedis() {
        return this.pool.getResource();
    }

    public RedisUtil init(String confPath) {
        final JedisPoolConfig config = new JedisPoolConfig();
        PropertiesUtil prop = PropertiesUtil.load(confPath);
        this.pool = new JedisPool(config,
                prop.getString("host", Protocol.DEFAULT_HOST),
                prop.getInt("port", Protocol.DEFAULT_PORT),
                prop.getInt("connectionTimeout", Protocol.DEFAULT_TIMEOUT),
                prop.getInt("soTimeout", Protocol.DEFAULT_TIMEOUT),
                prop.getString("password", null),
                prop.getInt("database", Protocol.DEFAULT_DATABASE),
                prop.getString("clientName", this.getClass().getName()),
                prop.getBoolean("ssl", false),
                null,
                null,
                null
        );
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
