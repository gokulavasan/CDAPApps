/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cask.twitter;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utility Class for (De)Serializing Map and for sorting Map based on value (score).
 */
public final class MapSerdesUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MapSerdesUtil.class);

  public static byte[] serializeMap(Map<String, Double> map) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try {
      ObjectOutputStream out = new ObjectOutputStream(byteOut);
      out.writeObject(map);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    return byteOut.toByteArray();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Double> deserializeMap(byte[] bytes) {
    if (bytes == null) {
      return Maps.newHashMap();
    }

    try {
      ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
      ObjectInputStream in = new ObjectInputStream(byteIn);
      return (Map<String, Double>) in.readObject();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Maps.newHashMap();
    }
  }

  @SuppressWarnings("unchecked")
  public static Map sortByComparator(Map unsortMap) {
    List list = new LinkedList(unsortMap.entrySet());
    //Sort list based on comparator
    Collections.sort(list, new Comparator() {
      public int compare(Object o1, Object o2) {
        return ((Comparable) ((Map.Entry) (o2)).getValue())
          .compareTo(((Map.Entry) (o1)).getValue());
      }
    });

    //Put sorted list into map again
    //LinkedHashMap retains the order in which keys were inserted
    Map sortedMap = new LinkedHashMap();
    Iterator it = list.iterator();
    while (it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      sortedMap.put(entry.getKey(), entry.getValue());
    }
    return sortedMap;
  }

  private MapSerdesUtil() {
  }
}
