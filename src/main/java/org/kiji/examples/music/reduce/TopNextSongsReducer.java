/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.examples.music.reduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;

import org.kiji.examples.music.SongCount;
import org.kiji.examples.music.TopSongs;
import org.kiji.mapreduce.AvroValueReader;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;


/**
 *
 */
public class TopNextSongsReducer extends KijiTableReducer<Text, AvroValue<SongCount>> implements
  AvroValueReader {
  private TreeSet<SongCount> mTopNextSongs;
  private int mNumberOfTopSongs = 10;
  private TopSongs mTopSongs;

  @Override
  public void setup(Context context) {
    mTopSongs = new TopSongs();

    mTopNextSongs = new TreeSet<SongCount>(new Comparator<SongCount>() {
      // Define a comparator
      @Override
      public int compare(SongCount song1, SongCount song2) {
        if (song1.getCount() > song2.getCount()) {
          return 1;
        } else if (song1.getCount() < song2.getCount()) {
          return -1;
        } else {
          return 0;
        }
      }
    });

  }

  @Override
  protected void reduce(Text key, Iterable<AvroValue<SongCount>> values, KijiTableContext context)
    throws IOException {
    // We are reusing objects, so we should make sure they are cleared for each new key.
    mTopNextSongs.clear();

    // Iterate through the song counts and keep track of the top N, where N is mNumberOfTopSongs,
    // counts that we see.
    for (AvroValue<SongCount> value : values) {
      // Remove AvroValue wrapper.
      SongCount currentSongCount = value.datum();
      // If the current SongCount is as large or larger the smallest SongCount in our sorted set,
      // we should add it to our sorted set
      if ( currentSongCount.getCount() >= mTopNextSongs.first().getCount()) {
        mTopNextSongs.add(currentSongCount);
        // If we now have too many elements in our sorted set, remove the element with the smallest
        // count.
        if (mTopNextSongs.size() >= mNumberOfTopSongs) {
          mTopNextSongs.pollFirst();
        }
      }
    }
    List<SongCount> topSongs = new LinkedList();
    topSongs.addAll(mTopNextSongs);
    mTopSongs.setTopSongs(topSongs);
    context.put(context.getEntityId(key.toString()), "info", "top_next_songs", mTopSongs);
  }

  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return SongCount.SCHEMA$;
  }


}
