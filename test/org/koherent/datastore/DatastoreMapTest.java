package org.koherent.datastore;

import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class DatastoreMapTest extends TestCase {
	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig());

	@Before
	public void setUp() {
		helper.setUp();
	}

	@After
	public void tearDown() {
		helper.tearDown();
	}

	@Test
	public void testBasics() {
		Map<String, Integer> map = new DatastoreMap<String, Integer>("kind");

		assertFalse(map.containsKey("abc"));
		assertFalse(map.containsKey("def"));
		assertFalse(map.containsKey("ghi"));
		assertEquals(null, map.get("abc"));
		assertEquals(null, map.get("def"));
		assertEquals(null, map.get("ghi"));

		assertTrue(map.isEmpty());
		assertEquals(0, map.size());
		map.put("abc", 123);
		assertFalse(map.isEmpty());
		assertEquals(1, map.size());
		map.put("def", 456);
		assertFalse(map.isEmpty());
		assertEquals(2, map.size());
		map.put("ghi", 789);
		assertFalse(map.isEmpty());
		assertEquals(3, map.size());

		assertTrue(map.containsKey("abc"));
		assertTrue(map.containsKey("def"));
		assertTrue(map.containsKey("ghi"));
		assertFalse(map.containsKey("xyz"));
		assertEquals(123, map.get("abc").intValue());
		assertEquals(456, map.get("def").intValue());
		assertEquals(789, map.get("ghi").intValue());
		assertEquals(null, map.get("xyz"));

		map.put("abc", 999);

		assertEquals(999, map.get("abc").intValue());

		assertFalse(map.isEmpty());
		assertEquals(3, map.size());
		map.remove("abc");
		assertFalse(map.isEmpty());
		assertEquals(2, map.size());
		map.remove("def");
		assertFalse(map.isEmpty());
		assertEquals(1, map.size());
		map.remove("ghi");
		assertTrue(map.isEmpty());
		assertEquals(0, map.size());

		assertFalse(map.containsKey("abc"));
		assertFalse(map.containsKey("def"));
		assertFalse(map.containsKey("ghi"));
		assertEquals(null, map.get("abc"));
		assertEquals(null, map.get("def"));
		assertEquals(null, map.get("ghi"));
	}
}
