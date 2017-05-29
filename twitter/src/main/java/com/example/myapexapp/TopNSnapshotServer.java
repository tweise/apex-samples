package com.example.myapexapp;

import java.util.ArrayList;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

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
/*
    {
      "id": 0.6760250790172551,
      "type": "dataQuery",
      "data": {
        "fields": [
          "hashtag",
          "count"
        ]
      },
      "countdown": 30,
      "incompleteResultOK": true
    }
*/
/*
    try {
      JSONObject query = new JSONObject();
      query.put("id", "topHashtags");
      query.put("type", "dataQuery");
      JSONObject data = new JSONObject();
      data.put("fields", getSchemaFields());
      query.put("data", data);
      //System.out.println(query);
      processQuery(query.toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
*/
    // emit results automatically, without need to send external query
    Fields fields = (super.getTableFieldToMapField() != null) ?
        new Fields(super.getTableFieldToMapField().values())
        : new Fields(getSchemaFields());
    DataQuerySnapshot query = new DataQuerySnapshot("topHashtags", fields);
    queryProcessor.enqueue(query, null, null);
  }

  private ArrayList<String> getSchemaFields() {
    // following does not retain order, go back to JSON
    // super.schema.getValuesDescriptor().getFields()
    ArrayList<String> fieldNames = new ArrayList<>();
    try {
      JSONObject schema = new JSONObject(super.snapshotSchemaJSON);
      JSONArray values = schema.getJSONArray(SnapshotSchema.FIELD_VALUES);
      for (int index = 0; index < values.length(); index++) {
        JSONObject value = values.getJSONObject(index);
        String name = value.getString(SnapshotSchema.FIELD_VALUES_NAME);
        fieldNames.add(name);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return fieldNames;
  }

}
