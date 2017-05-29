package com.example.myapexapp;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.SnapshotSchema;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;

/**
 *  An extension of {@link AppDataSnapshotServerMap} that automatically emits results.
 */
public class TopNSnapshotServer extends AppDataSnapshotServerMap
{
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    // emit results automatically, without need to send external query
    Fields fields = (super.getTableFieldToMapField() != null) ?
        new Fields(super.getTableFieldToMapField().values())
        : super.schema.getValuesDescriptor().getFields();
    DataQuerySnapshot query = new DataQuerySnapshot("topHashtags", fields);
    queryProcessor.enqueue(query, null, null);
  }

  @VisibleForTesting
  protected SnapshotSchema getSchema() {
    return super.schema;
  }

}
