/*
 * Copyright (c) 2015 DataTorrent
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.dimensions.mysql.app;

import java.net.URI;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.dimensions.mysql.JDBCDimensionalOutputOperator;

/**
 * This demo requires five tables in Mysql.
 *
 * Create the meta data table:
 *
 * CREATE TABLE dt_out (appID varchar(255) NOT NULL DEFAULT '', operatorID int(11) NOT NULL DEFAULT '0', windowID bigint(20) NOT NULL DEFAULT '0', PRIMARY KEY (appID,operatorID,windowID)) ENGINE=MyISAM DEFAULT CHARSET=latin1
 *
 * Create the time only dimension combination table
 *
 * CREATE TABLE no_key (time bigint(20) NOT NULL DEFAULT '0', count bigint(20) DEFAULT NULL, PRIMARY KEY (time)) ENGINE=MyISAM DEFAULT CHARSET=latin1
 *
 * Create the node_name only dimension combination table
 *
 * CREATE TABLE node_name (time bigint(20) NOT NULL DEFAULT '0', node_name varchar(255) NOT NULL DEFAULT '', count bigint(20) DEFAULT NULL, PRIMARY KEY (time,node_name)) ENGINE=MyISAM DEFAULT CHARSET=latin1
 *
 * Create the node_type only dimension combination table.
 *
 * CREATE TABLE node_type (time bigint(20) NOT NULL DEFAULT '0', node_type varchar(255) NOT NULL DEFAULT '', count bigint(20) DEFAULT NULL, PRIMARY KEY (time,node_type)) ENGINE=MyISAM DEFAULT CHARSET=latin1
 *
 * Create the node_name_node_type dimension combination table
 *
 * CREATE TABLE node_name_type (time bigint(20) NOT NULL DEFAULT '0', node_name varchar(255) NOT NULL DEFAULT '', node_type varchar(255) NOT NULL DEFAULT '', count bigint(20) DEFAULT NULL, PRIMARY KEY (time,node_name,node_type)) ENGINE=MyISAM DEFAULT CHARSET=latin1
 */
@ApplicationAnnotation(name=MysqlOutputDemo.APP_NAME)
public class MysqlOutputDemo implements StreamingApplication
{
  public static final String APP_NAME = "MYSQL_Dimensions_Output";
  public static final String EVENT_SCHEMA = "mysqlSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    RandomTupleGenerator input = dag.addOperator("InputGenerator", RandomTupleGenerator.class);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions =
    dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    TFileImpl hdsFile = new TFileImpl.DTFileImpl();

    store.setFileStore(hdsFile);
    store.setConfigurationSchemaJSON(eventSchema);

    dimensions.setConfigurationSchemaJSON(eventSchema);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("node_type", "getNodeType()");
    keyToExpression.put("node_name", "getNodeName()");
    keyToExpression.put("time", "getTimestamp()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("count", "getNodeName()");

    dimensions.setKeyToExpression(keyToExpression);
    dimensions.setAggregateToExpression(aggregateToExpression);

    ////

    JDBCDimensionalOutputOperator jdbc = dag.addOperator("JDBCOutput", JDBCDimensionalOutputOperator.class);

    Map<Integer, Map<String, String>> tableNames = Maps.newHashMap();
    List<String> tableNamesList = Lists.newArrayList("no_key", "node_name", "node_type", "node_name_type");

    for(int ddID = 0; ddID < 4; ddID++) {
      Map<String, String> aggToTableName = Maps.newHashMap();
      aggToTableName.put("COUNT", tableNamesList.get(ddID));
      tableNames.put(ddID, aggToTableName);
    }

    jdbc.setTableNames(tableNames);
    jdbc.setEventSchema(eventSchema);
    jdbc.setBatchSize(100);

    JdbcTransactionalStore jdbcStore = new JdbcTransactionalStore();
    jdbcStore.setMetaTable("dt_out");
    jdbcStore.setMetaTableAppIdColumn("appID");
    jdbcStore.setMetaTableOperatorIdColumn("operatorID");
    jdbcStore.setMetaTableWindowColumn("windowID");
    jdbcStore.setDatabaseDriver("com.mysql.jdbc.Driver");
    jdbcStore.setDatabaseUrl("jdbc:mysql://node17.morado.com:5505/tim");
    jdbcStore.setUserName("tim");
    jdbcStore.setPassword("isClever");

    jdbc.setStore(jdbcStore);

    ////

    Operator.OutputPort<String> queryPort;
    Operator.InputPort<String> queryResultPort;

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    PubSubWebSocketAppDataQuery wsIn = new PubSubWebSocketAppDataQuery();
    wsIn.setUri(uri);
    wsIn.setTopic("MYSQLQuery");
    queryPort = wsIn.outputPort;

    dag.addOperator("Query", wsIn);
    dag.addStream("Query", queryPort, store.query).setLocality(Locality.CONTAINER_LOCAL);

    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(uri);
    wsOut.setTopic("MYSQLResult");
    queryResultPort = wsOut.input;

    dag.addStream("QueryResult", store.queryResult, queryResultPort);

    dag.addStream("ConvertStream", input.output, dimensions.input);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("JDBCOutput", store.updates, jdbc.input);
  }
}
