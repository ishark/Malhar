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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class RedisInputOperatorTest {
  private RedisStore operatorStore;
  private RedisStore testStore;

  public static class CollectorModule extends BaseOperator {
    volatile static List<KeyValPair<String, String>> resultMap = new ArrayList<KeyValPair<String, String>>();
    static long resultCount = 0;

    public final transient DefaultInputPort<KeyValPair<String, String>> inputPort = new DefaultInputPort<KeyValPair<String, String>>() {
      @Override
      public void process(KeyValPair<String, String> tuple) {
        resultMap.add(tuple);
        resultCount++;
      }

    };
  }

  @Test
  public void testIntputOperator() throws IOException {

    this.operatorStore = new RedisStore();
    this.testStore = new RedisStore();

    testStore.connect();
    ScanParams params =  new ScanParams();
    params.count(1);
    
    testStore.put("test_abc", "789");
    testStore.put("test_def", "456");
    testStore.put("test_ghi", "123");
        
    
    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      RedisKeyValueInputOperator inputOperator = dag.addOperator("input",
          new RedisKeyValueInputOperator());
      final CollectorModule collector = dag.addOperator("collector",
          new CollectorModule());

      inputOperator.setStore(operatorStore);
      dag.addStream("stream", inputOperator.outputPort, collector.inputPort);
      final LocalMode.Controller lc = lma.getController();

      new Thread("LocalClusterController") {
        @Override
        public void run() {
          long startTms = System.currentTimeMillis();
          long timeout = 10000L;
          try {
            Thread.sleep(1000);
            while (System.currentTimeMillis() - startTms < timeout) {
              if (CollectorModule.resultMap.size() < 3) {
                Thread.sleep(10);
              } else {
                break;
              }
            }
          } catch (InterruptedException ex) {
          }
          lc.shutdown();
        }
      }.start();

      lc.run();

      Assert.assertTrue(CollectorModule.resultMap
          .contains(new KeyValPair<String, String>("test_abc", "789")));
      Assert.assertTrue(CollectorModule.resultMap
          .contains(new KeyValPair<String, String>("test_def", "456")));
      Assert.assertTrue(CollectorModule.resultMap
          .contains(new KeyValPair<String, String>("test_ghi", "123")));
    }

    finally {
      for (KeyValPair<String, String> entry : CollectorModule.resultMap) {
    	System.out.println("entry is  = " + entry.getKey() + " : " + entry.getValue());
        testStore.remove(entry.getKey());
      }

      testStore.disconnect();
    }
  }
}
