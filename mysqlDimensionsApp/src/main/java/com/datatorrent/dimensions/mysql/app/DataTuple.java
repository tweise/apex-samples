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

public class DataTuple
{
  private long timestamp;
  private String nodeName;
  private String nodeType;

  public DataTuple()
  {
  }

  public DataTuple(long timestamp,
                   String nodeName,
                   String nodeType)
  {
    this.timestamp = timestamp;
    this.nodeName = nodeName;
    this.nodeType = nodeType;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp()
  {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  /**
   * @return the nodeName
   */
  public String getNodeName()
  {
    return nodeName;
  }

  /**
   * @param nodeName the nodeName to set
   */
  public void setNodeName(String nodeName)
  {
    this.nodeName = nodeName;
  }

  /**
   * @return the nodeType
   */
  public String getNodeType()
  {
    return nodeType;
  }

  /**
   * @param nodeType the nodeType to set
   */
  public void setNodeType(String nodeType)
  {
    this.nodeType = nodeType;
  }
}
