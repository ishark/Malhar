/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.redis;

import java.util.ArrayList;
import java.util.List;
import com.datatorrent.lib.util.KeyValPair;

/**
 * This is the an implementation of a Redis input operator for fetching
 * Key-Value pair stored in Redis. It takes in keys to fetch and emits
 * corresponding <Key, Value> Pair. Value data type is String in this case.
 * <p>
 * </p> 
 * @displayName Redis Input Operator for Key Value pair
 * @category Store
 * @tags input operator, key value
 *
 */
public class RedisKeyValueInputOperator extends
    AbstractRedisInputOperator<KeyValPair<String, String>> {

  private List<Object> keysObjectList = new ArrayList<Object>();

  @Override
  public void beginWindow(long windowId) {
    super.beginWindow(windowId);
  }

  @Override
  public void processTuples() {
    keysObjectList = new ArrayList<Object>(keys);
    if (keysObjectList.size() > 0) {

      List<Object> allValues = store.getAll(keysObjectList);
      for (int i = 0; i < allValues.size() && i < keys.size(); i++) {
        outputPort.emit(new KeyValPair<String, String>(keys.get(i), allValues
            .get(i).toString()));
      }
      keys.clear();
      keysObjectList.clear();
    }
  }
}
