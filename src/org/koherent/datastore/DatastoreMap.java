package org.koherent.datastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

public class DatastoreMap<K, V> implements Map<K, V> {
	private static final String PROPERTY_NAME = "v";

	private String kind;

	public DatastoreMap(String kind) {
		this.kind = kind;
	}

	@Override
	public int size() {
		int size = 0;

		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		Query query = new Query(kind);
		query.setKeysOnly();
		for (Iterator<Entity> iterator = datastore.prepare(query).asIterator(); iterator
				.hasNext(); iterator.next()) {
			size++;
		}

		return size;
	}

	@Override
	public boolean isEmpty() {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		Query query = new Query(kind);
		query.setKeysOnly();

		return !datastore.prepare(query).asIterator().hasNext();
	}

	@Override
	public boolean containsKey(Object key) {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		try {
			datastore.get(KeyFactory.createKey(kind, key.toString()));
			return true;
		} catch (EntityNotFoundException e) {
			return false;
		}
	}

	@Override
	public boolean containsValue(Object value) {
		for (V containedValue : values()) {
			if (containedValue.equals(value)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public V get(Object key) throws DatastoreFailureException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		try {
			return toValue(datastore.get(KeyFactory.createKey(kind,
					key.toString())));
		} catch (EntityNotFoundException e) {
			return null;
		}
	}

	@Override
	public V put(K key, V value) throws DatastoreFailureException {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();
		datastore.put(toEntity(key, value));

		return null; // does not satisfy the specification of Map for the performance
	}

	@Override
	public V remove(Object key) {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();
		datastore.delete(KeyFactory.createKey(kind, key.toString()));

		return null; // does not satisfy the specification of Map for the performance
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void clear() {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		List<Key> keys = new ArrayList<Key>();
		Query query = new Query(kind);
		query.setKeysOnly();
		for (Iterator<Entity> iterator = datastore.prepare(query).asIterator(); iterator
				.hasNext();) {
			keys.add(iterator.next().getKey());
		}

		datastore.delete(keys);
	}

	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> values() {
		DatastoreService datastore = DatastoreServiceFactory
				.getDatastoreService();

		List<V> values = new ArrayList<V>();
		Query query = new Query(kind);
		for (Iterator<Entity> iterator = datastore.prepare(query).asIterator(); iterator
				.hasNext();) {
			values.add(toValue(iterator.next()));
		}

		return Collections.unmodifiableCollection(values);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	private Entity toEntity(K key, V value) {
		Entity entity = new Entity(kind, key.toString());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOut = new ObjectOutputStream(out);
			objectOut.writeObject(value);
			objectOut.flush();
		} catch (IOException e) {
			throw new Error("Never reaches here.");
		}
		entity.setUnindexedProperty(PROPERTY_NAME, new Blob(out.toByteArray()));

		return entity;
	}

	@SuppressWarnings("unchecked")
	private V toValue(Entity entity) {
		ByteArrayInputStream in = new ByteArrayInputStream(
				((Blob) entity.getProperty(PROPERTY_NAME)).getBytes());
		ObjectInputStream objectIn;
		try {
			objectIn = new ObjectInputStream(in);
			return (V) objectIn.readObject();
		} catch (IOException e) {
			throw new Error("Never reaches here.");
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
}
