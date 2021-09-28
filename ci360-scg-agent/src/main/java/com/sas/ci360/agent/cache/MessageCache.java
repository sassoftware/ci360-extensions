/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.cache;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.json.JSONObject;

import com.sas.ci360.agent.exceptions.ConfigurationException;

public class MessageCache {
	private static final boolean PERSISTENT = true;
	private PersistentCacheManager cacheManager;
	private Cache<String, String> cache;
	
	public MessageCache() {

	}
	
	public MessageCache(Properties config) throws ConfigurationException {
		this();
		
		if (config.getProperty("agent.cache.messageCacheName") == null) throw new ConfigurationException("Missing required configuration property agent.cache.messageCacheName");
		if (config.getProperty("agent.cache.cacheDirectory") == null) throw new ConfigurationException("Missing required configuration property agent.cache.cacheDirectory");
		if (config.getProperty("agent.cache.messageCacheHeap") == null) throw new ConfigurationException("Missing required configuration property agent.cache.messageCacheHeap");
		if (config.getProperty("agent.cache.messageCacheOffHeapMB") == null) throw new ConfigurationException("Missing required configuration property agent.cache.messageCacheOffHeapMB");
		if (config.getProperty("agent.cache.messageCacheDiskMB") == null) throw new ConfigurationException("Missing required configuration property agent.cache.messageCacheDiskMB");
		if (config.getProperty("agent.cache.messageCacheTTLMin") == null) throw new ConfigurationException("Missing required configuration property agent.cache.messageCacheTTLMin");
		
		final String cacheName = config.getProperty("agent.cache.messageCacheName");
		final String cacheStoreName = cacheName + "Data";
		final String cacheDir = config.getProperty("agent.cache.cacheDirectory");
		final long cacheHeap = Long.parseLong(config.getProperty("agent.cache.messageCacheHeap"));
		final long cacheOffHeap = Long.parseLong(config.getProperty("agent.cache.messageCacheOffHeapMB"));
		final long cacheDisk = Long.parseLong(config.getProperty("agent.cache.messageCacheDiskMB"));
		final long timeToLiveMin = Long.parseLong(config.getProperty("agent.cache.messageCacheTTLMin"));
		System.out.println("Cache config: name: " + cacheName + ", cacheDir: " + cacheDir + ", heap: " + cacheHeap + ", offheapMB: " + cacheOffHeap + ", diskMB: " + cacheDisk + ", TTL: " + timeToLiveMin);
		
		CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, 
				ResourcePoolsBuilder.newResourcePoolsBuilder()
					.heap(cacheHeap, EntryUnit.ENTRIES)
					.disk(cacheDisk, MemoryUnit.MB, PERSISTENT))
					.withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(timeToLiveMin)))
				.build();
		
		this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
				.with(CacheManagerBuilder.persistence(new File(cacheDir, cacheStoreName)))
				.withCache(cacheName, cacheConfiguration)
				.build(true);
		this.cache = cacheManager.getCache(cacheName, String.class, String.class);
	}
	
	public void close() {
		this.cacheManager.close();
	}
	
	public void put(String msgId, JSONObject msgData) {
		this.cache.put(msgId, msgData.toString());
	}
	
	public JSONObject get(String msgId) {
		try {
			return new JSONObject(cache.get(msgId));
		} catch (Exception ex) {

		}
		return null;
	}
	
	public void remove(String msgId) {
		this.cache.remove(msgId);
	}
	
	public List<String> getKeys() {
		List<String> keys = new ArrayList<String>();
		
		for (Cache.Entry<String, String> entry : this.cache) {
		    keys.add(entry.getKey());
		}

		return keys;
	}
}
