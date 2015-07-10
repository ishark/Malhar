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

import javax.validation.constraints.NotNull;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;

/**
 * This is the base implementation of a Redis input operator.
 * <p>
 * </p>
 * 
 * @displayName Abstract Redis Input
 * @category Store
 * @tags input operator, key value
 *
 * @param <T>
 *          The tuple type.
 * @since 0.9.3
 */
public abstract class AbstractRedisInputOperator<T> extends
    AbstractStoreInputOperator<T, RedisStore> implements CheckpointListener {
  protected transient List<String> keys = new ArrayList<String>();
  protected transient String scanOffset;
  protected transient ScanParams scanParameters;
  private transient boolean scanComplete;
  private transient Integer recoveryOffset, backupOffset;
  private transient int scanCount;

  @NotNull
  protected IdempotentStorageManager idempotentStorageManager;

  private transient OperatorContext context;
  private transient long currentWindowId;

  public AbstractRedisInputOperator() {
    scanCount = 2;
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
  }

  @Override
  public void beginWindow(long windowId) {
    currentWindowId = windowId;
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        Integer recoveredOffset = (Integer) idempotentStorageManager.load(
            context.getId(), windowId);
        if (recoveredOffset != null) {
          scanOffset = recoveredOffset.toString();
        }
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
    
    recoveryOffset = backupOffset;
    scanKeysFromOffset();
  }

  private void scanKeysFromOffset() {
    if (!scanComplete) {
      ScanResult<String> result = store.ScanKeys(scanOffset, scanParameters);
      backupOffset = Integer.parseInt(scanOffset);
      scanOffset = result.getStringCursor();
      if(scanOffset.equalsIgnoreCase("0"))
      {
        scanComplete = true;
      }
      if (result.getStringCursor().equals("0")) {
        // Redis store returns 0 after all data is read,
        // point scanOffset to the end in this case for reading any new tuples
        Integer endOffset = backupOffset + result.getResult().size();
        scanOffset = endOffset.toString();
      }

      keys = result.getResult();
    }
  }

  @Override
  public void setup(OperatorContext context) {
    super.setup(context);
    idempotentStorageManager.setup(context);
    this.context = context;
    scanOffset = "0";
    scanComplete = false;
    scanParameters = new ScanParams();
    scanParameters.count(scanCount);
  }

  @Override
  public void endWindow() {
    super.endWindow();

    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        idempotentStorageManager.save(recoveryOffset, context.getId(),
            currentWindowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void teardown() {
    super.teardown();
    idempotentStorageManager.teardown();
  }

  public int getScanCount() {
    return scanCount;
  }

  public void setScanCount(int scanCount) {
    this.scanCount = scanCount;
  }

  @Override
  public void emitTuples() {
    processTuples();
    scanKeysFromOffset();
  }

  abstract public void processTuples();

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(context.getId(), windowId);
    }
    catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }
}
