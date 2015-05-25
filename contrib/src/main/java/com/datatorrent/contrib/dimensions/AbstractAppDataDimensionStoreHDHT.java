/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.appdata.query.QueryManager;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.query.serde.Message;
import com.datatorrent.lib.appdata.query.serde.Result;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorUtils;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAppDataDimensionStoreHDHT extends DimensionsStoreHDHT
{
  @NotNull
  protected ResultFormatter resultFormatter = new ResultFormatter();
  @NotNull
  protected AggregatorRegistry aggregatorInfo = AggregatorUtils.DEFAULT_AGGREGATOR_REGISTRY;

  //Query Processing - Start
  protected transient QueryManager<DataQueryDimensional, QueryMeta, MutableLong, Result> queryProcessor;
  protected final transient MessageDeserializerFactory queryDeserializerFactory;

  @VisibleForTesting
  public SchemaRegistry schemaRegistry;
  protected transient MessageSerializerFactory resultSerializerFactory;

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      LOG.debug("Received {}", s);
      Message query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch (IOException ex) {
        LOG.error("error parsing query {}", s, ex);
        return;
      }

      if (query instanceof SchemaQuery) {
        processSchemaQuery((SchemaQuery) query);
      }
      else if (query instanceof DataQueryDimensional) {
        processDimensionalDataQuery((DataQueryDimensional) query);
      }
      else {
        LOG.error("Invalid query {}", s);
      }
    }
  };

  @SuppressWarnings("unchecked")
  public AbstractAppDataDimensionStoreHDHT()
  {
    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    aggregatorInfo.setup();

    schemaRegistry = getSchemaRegistry();

    //setup query processor
    queryProcessor = QueryManager.newInstance(new DimensionsQueryExecutor(this, schemaRegistry),
      new DimensionsQueueManager(this, schemaRegistry));
    queryProcessor.setup(context);

    resultSerializerFactory = new MessageSerializerFactory(resultFormatter);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryProcessor.beginWindow(windowId);
    super.beginWindow(windowId);
  }

  protected void processDimensionalDataQuery(DataQueryDimensional dataQueryDimensional)
  {
    queryProcessor.enqueue(dataQueryDimensional, null, null);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    Result aotr;

    while ((aotr = queryProcessor.process()) != null) {
      String result = resultSerializerFactory.serialize(aotr);
      LOG.debug("Emitting the result: {}", result);
      queryResult.emit(result);
    }
    queryProcessor.endWindow();
  }

  @Override
  public void teardown()
  {
    queryProcessor.teardown();
    super.teardown();
  }

  /**
   * Processes schema queries
   *
   * @param schemaQuery a schema query
   */
  protected abstract void processSchemaQuery(SchemaQuery schemaQuery);

  /**
   * @return the schema registry
   */
  protected abstract SchemaRegistry getSchemaRegistry();

  @Override
  public IncrementalAggregator getAggregator(int aggregatorID)
  {
    return aggregatorInfo.getIncrementalAggregatorIDToAggregator().get(aggregatorID);
  }

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return aggregatorInfo.getIncrementalAggregatorNameToID().get(aggregatorName);
  }

  public void setAppDataFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  /**
   * @return the resultFormatter
   */
  public ResultFormatter getAppDataFormatter()
  {
    return resultFormatter;
  }

  /**
   * @return the aggregatorInfo
   */
  public AggregatorRegistry getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  /**
   * @param aggregatorInfo the aggregatorInfo to set
   */
  public void setAggregatorInfo(@NotNull AggregatorRegistry aggregatorInfo)
  {
    this.aggregatorInfo = aggregatorInfo;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataDimensionStoreHDHT.class);
}
