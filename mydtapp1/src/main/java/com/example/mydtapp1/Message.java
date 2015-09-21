/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.mydtapp1;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;

public class Message implements Event, Bucketable
{
  private String key;
  private long time;

  private Message()
  {
  }

  public Message(String key, long time)
  {
    this.key = key;
    this.time = time;
  }

  @Override
  public long getTime()
  {
    return time;
  }

  @Override
  public Object getEventKey()
  {

    return key;
  }
}
