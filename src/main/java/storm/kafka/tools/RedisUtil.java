package storm.kafka.tools;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;


public final class RedisUtil {

	private static JedisPool pool = null;

	/**
	 * 构建redis连接池
	 * 
	 * @param ip
	 * @param port
	 * @return JedisPool
	 */
	public static JedisPool getPool() {
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
			// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
			config.setMaxTotal(300);
			// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
			config.setMaxIdle(5);
			// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
			config.setMaxWaitMillis(1000 * 100);
			// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
			config.setTestOnBorrow(true);
			pool = new JedisPool(config, Constant.REDIS_HOST,
					Constant.REDIS_PORT,1000 * 60*2);
		}
		return pool;
	}

	/**
	 * 返还到连接池
	 * 
	 * @param pool
	 * @param redis
	 */
	@SuppressWarnings("deprecation")
	public static void returnResource(JedisPool pool, Jedis redis) {
		if (redis != null) {
			pool.returnResource(redis);
		}
	}

	public static List<String> popMap(String key, final String... fields) {
		List<String> value = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			value = jedis.hmget(key, fields);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return value;
	}

	public static void pushMap(String key, Map<String,String> map) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			jedis.hmset(key, map);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static Long expire(final String key, final int seconds) {
		long l = 0;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			l = jedis.expire(key, seconds);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return l;
	}
	 /**
     * 获取Jedis实例
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (pool != null) {
                Jedis resource = pool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
	public static Map<String, String> keysMap(String key) {
		Map<String, String> value = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			Iterator<String> iter = jedis.hkeys(key).iterator();
			value = new HashMap<String, String>();
			while (iter.hasNext()) {
				String mapkey = iter.next();
				List<String> values = jedis.hmget(key, mapkey);
				value.put(mapkey, values.get(0));
			}
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}

		return value;
	}
	
	public static void del(String key) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			jedis.del(key);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}
	public static void addSet(String key, String... members) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			jedis.sadd(key, members);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}
	public static boolean isMemberInSet(String key, String member) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			return jedis.sismember(key, member);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return false;
	}
	public static int UnionSetSize(String[] keys) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			if(keys.length<0){
				return jedis.sunion(keys).size();
			}else{
				//空间换时间
				String tmpKey=UUID.randomUUID().toString();
				long l=jedis.sunionstore(tmpKey, keys);
				jedis.del(tmpKey);
				return new Long(l).intValue();
			}
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return 0;
	}
	public static String redisMemoryInfo() {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			return jedis.info("Memory");
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return "";
	}
	public static Double SortedsetIncrby(String key, double score, String member) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			return jedis.zincrby(key, score, member);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return null;
	}
	public static Set<Tuple> SortedsetRevrangeWithScores(String key, long start, long end) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			return jedis.zrevrangeWithScores(key, start, end);
		} catch (Exception e) {
			// 释放redis对象
			returnResource(pool, jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return null;
	}
}