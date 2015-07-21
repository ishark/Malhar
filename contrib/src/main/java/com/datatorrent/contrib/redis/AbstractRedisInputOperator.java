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
  protected transient Integer scanOffset;
  protected transient ScanParams scanParameters;
  private transient boolean scanComplete;
  private transient Integer backupOffset;
  // recoveryOffset contains last offset processed in window
  private Integer recoveryOffset;
  private int scanCount;
  private transient boolean replay;

  @NotNull
  private IdempotentStorageManager idempotentStorageManager;

  private transient OperatorContext context;
  private transient long currentWindowId;
  private transient Integer sleepTimeMillis;

  public AbstractRedisInputOperator() {
    scanCount = 2;
    recoveryOffset = 0;
    setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
  }

  @Override
  public void beginWindow(long windowId) {
    currentWindowId = windowId;
    replay = false;
    if (currentWindowId <= getIdempotentStorageManager()
        .getLargestRecoveryWindow()) {
      try {
        if (checkIfWindowExistsInIdempotencyManager(windowId - 1)) {
          // Begin offset for this window is recovery offset stored for the last window
          Integer recoveredOffset = (Integer) getIdempotentStorageManager()
              .load(context.getId(), windowId - 1);
          if (recoveredOffset != null) {
            scanOffset = recoveredOffset;
          }
          replay = true;
        }
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  private boolean checkIfWindowExistsInIdempotencyManager(long windowId)
      throws IOException {
    long[] windowsIds = getIdempotentStorageManager().getWindowIds(
        context.getId());
    for (long id : windowsIds) {
      if (windowId == id)
        return true;
    }
    return false;
  }

  private void scanKeysFromOffset() {
    if (!scanComplete) {
      if (replay && scanOffset > recoveryOffset) {
        try {
          Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
          DTThrowable.rethrow(e);
        }
        return;
      }

      ScanResult<String> result = store.ScanKeys(scanOffset, scanParameters);
      backupOffset = scanOffset;
      scanOffset = Integer.parseInt(result.getStringCursor());
      if (scanOffset == 0) {
        scanComplete = true;

        // Redis store returns 0 after all data is read,
        // point scanOffset to the end in this case for reading any new tuples
        scanOffset = backupOffset + result.getResult().size();
      }

      keys = result.getResult();
    }
  }

  @Override
  public void setup(OperatorContext context) {
    super.setup(context);
    sleepTimeMillis = context.getValue(context.SPIN_MILLIS);
    getIdempotentStorageManager().setup(context);
    this.context = context;
    scanOffset = 0;
    scanComplete = false;
    scanParameters = new ScanParams();
    scanParameters.count(scanCount);
    // For the 1st window after checkpoint, windowID - 1 would not have recovery
    // offset stored in idempotentStorageManager
    // But recoveryOffset is non-transient, so will be recovered with
    // checkPointing
    scanOffset = recoveryOffset;
  }

  @Override
  public void endWindow() {
    while (replay && scanOffset < recoveryOffset) {
      // If less keys got scanned in this window, scan till recovery offset
      scanKeysFromOffset();
      processTuples();
    }
    super.endWindow();
    recoveryOffset = scanOffset;

    if (currentWindowId > getIdempotentStorageManager()
        .getLargestRecoveryWindow()) {
      try {
        getIdempotentStorageManager().save(recoveryOffset, context.getId(),
            currentWindowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void teardown() {
    super.teardown();
    getIdempotentStorageManager().teardown();
  }

  public int getScanCount() {
    return scanCount;
  }

  public void setScanCount(int scanCount) {
    this.scanCount = scanCount;
  }

  @Override
  public void emitTuples() {
    scanKeysFromOffset();
    processTuples();
  }

  abstract public void processTuples();

  @Override
  public void checkpointed(long windowId) {
  }

  @Override
  public void committed(long windowId) {
    try {
      getIdempotentStorageManager().deleteUpTo(context.getId(), windowId);
    } catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }

  public IdempotentStorageManager getIdempotentStorageManager() {
    return idempotentStorageManager;
  }

  public void setIdempotentStorageManager(
      IdempotentStorageManager idempotentStorageManager) {
    this.idempotentStorageManager = idempotentStorageManager;
  }
}
