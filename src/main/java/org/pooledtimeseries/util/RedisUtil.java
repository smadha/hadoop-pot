/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pooledtimeseries.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisUtil {
	private static final String HOST = "localhost";
	private static final int PORT = 6379;
	private static boolean redisEnabled;

	private static JedisPool redisPool = new JedisPool(new JedisPoolConfig(), HOST, PORT, Protocol.DEFAULT_TIMEOUT);

	private static final Logger LOG = Logger.getLogger(RedisUtil.class.getName());

	static {
		LOG.info("Checking Redis config");
		Jedis jedis = null;
		try {
			jedis = redisPool.getResource();
			// Is connected
			redisEnabled = true;
			LOG.info("Redis enabled");
		} catch (JedisConnectionException e) {
			redisEnabled = false;
			if (jedis != null) {
				redisPool.returnBrokenResource(jedis);
				jedis = null;
			}
			LOG.log(Level.SEVERE, "REDIS DISABLED", e);
		} finally {
			if (jedis != null) {
				redisPool.returnResource(jedis);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		setObjectInRedis("key", "value");
		LOG.info("Set test key in Redis - " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		LOG.info(getObjectFromRedis("key").toString());
		LOG.info("Get test key in Redis - " + (System.currentTimeMillis() - start));

		redisPool.destroy();
	}

	/**
	 * Lookup key in cache and return deserialized byte[]
	 */
	public static Object getObjectFromRedis(String key) {
		if(!isRedisEnabled())
			return null;
		
		byte[] byteArr = getKeyRedis(key.getBytes());
		// Miss
		if (byteArr == null || byteArr.length == 0) {
			return null;
		}
		long start = System.currentTimeMillis();
		ByteArrayInputStream bis = new ByteArrayInputStream(byteArr);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			LOG.fine(key + "\nTime taken deserializing - " + (System.currentTimeMillis() - start));
			return in.readObject();
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to deserialize", e);
			return null;
		} finally {
			try {
				bis.close();
			} catch (Exception ex) {
				// ignore close exception
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception ex) {
				// ignore close exception
			}
		}

	}

	/**
	 * Set key, value in cache after serializing value asynchronously
	 */
	public static void setObjectInRedisAsync(final String key, final Object value) {
		
		if(!isRedisEnabled())
			return;
					
		new Thread() {
			public void run() {
				setObjectInRedis(key, value);
			};
		}.start();
	}

	/**
	 * Set key, value in cache after serializing value synchronously
	 */
	public static String setObjectInRedis(String key, Object value) {
		if(!isRedisEnabled())
			return null;
		
		long start = System.currentTimeMillis();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] byteArr = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(value);
			byteArr = bos.toByteArray();
			LOG.fine(key + "\nTime taken serializing - " + (System.currentTimeMillis() - start));
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to serialize", e);

		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (Exception ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (Exception ex) {
				// ignore close exception
			}
		}

		if (byteArr != null) {
			start = System.currentTimeMillis();
			return setKeyRedis(key.getBytes(), byteArr);
		} else {
			return null;
		}
	}

	/**
	 * Set key, value in cache
	 */
	public static String setKeyRedis(byte[] key, byte[] value) {
		Jedis jedis = null;
		try {
			long start = System.currentTimeMillis();
			jedis = redisPool.getResource();
			String res = jedis.set(key, value);
			LOG.fine(new String(key) + "\nTime taken set- " + (System.currentTimeMillis() - start));
			return res;
		} catch (JedisConnectionException e) {
			if (jedis != null) {
				redisPool.returnBrokenResource(jedis);
				jedis = null;
			}
			LOG.log(Level.SEVERE, "Unable to set key in REDIS", e);
			return null;
		} finally {
			if (jedis != null) {
				redisPool.returnResource(jedis);
			}
		}
	}

	/**
	 * lookup key in cache and return value as byte[]
	 */
	public static byte[] getKeyRedis(byte[] key) {
		Jedis jedis = null;
		try {
			long start = System.currentTimeMillis();
			jedis = redisPool.getResource();
			byte[] obj = jedis.get(key);
			LOG.fine(new String(key) + "\nTime taken get- " + (System.currentTimeMillis() - start));
			return obj;

		} catch (JedisConnectionException e) {
			if (jedis != null) {
				redisPool.returnBrokenResource(jedis);
				jedis = null;
			}
			LOG.log(Level.SEVERE, "Unable to get key from REDIS", e);
			return null;
		} finally {
			if (jedis != null) {
				redisPool.returnResource(jedis);
			}
		}
	}

	/**
	 * Returns true if Redis is configured
	 * 
	 * @return
	 */
	public static boolean isRedisEnabled() {
		return redisEnabled;
	}

}