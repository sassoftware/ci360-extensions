package com.sas.ci360.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Assert;
import org.junit.Test;

public class EhCacheTest {
	private Cache<String, String> cache;

	@Test
	public void testCache() throws Exception {
		CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
				.withCache("cache1", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
						ResourcePoolsBuilder.heap(100).offheap(1, MemoryUnit.MB)))
				.build(true);
		cache = cacheManager.getCache("cache1", String.class, String.class);
		
		cache.put("1", "Jan");
		cache.put("2", "Feb");
		cache.put("3", "Mar");

		String value = cache.get("2");
		Assert.assertEquals("Feb", value);

		Assert.assertTrue(cache.containsKey("3"));
		Assert.assertFalse(cache.containsKey("10"));

	}
}
