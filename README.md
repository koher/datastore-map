datastore-map
========================

_datastore-map_ provides the `DatastoreMap` class that is a `Map` for getting/putting entries from/to  the Datastore for [GAE/J](https://cloud.google.com/appengine/docs/java/).

```java
Map<String, Integer> map = new DatastoreMap<String, Integer>("kind");

map.put("abc", 123);
map.put("def", 456);
map.put("ghi", 789);

map.get("abc"); // 123
map.get("def"); // 456
map.get("ghi"); // 789
map.get("xyz"); // null

map.containsKey("abc"); // true
map.containsKey("def"); // true
map.containsKey("ghi"); // true
map.containsKey("xyz"); // false

map.isEmpty(); // false

map.remove("abc");
map.remove("def");
map.remove("ghi");

map.isEmpty(); // true
```

License
------------------------

[The MIT License](LICENSE)
