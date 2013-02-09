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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.common.flags.Flag;
import org.kiji.examples.music.NextSongCount;
import org.kiji.examples.music.SongBiGram;
import org.kiji.mapreduce.AvroKeyReader;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;

/**
 * A reducer that will sum the values associated with a key and write that sum
 * to a column specified via command line flags.
 */
public class SequentialPlayCountTableReducer
    extends KijiTableReducer<AvroKey<SongBiGram>, LongWritable>
    implements AvroKeyReader {

  /** {@inheritDoc} */
  @Override
  protected void reduce(AvroKey<SongBiGram> key, Iterable<LongWritable> values, KijiTableContext context)
      throws IOException {
    // Initialize sum to zero.
    long sum = 0L;
    // Add up all the values.
    for (LongWritable value : values) {
      sum += value.get();
    }
    // Set values for this count.
    NextSongCount nextCount = new NextSongCount();
    nextCount.setCount(sum);
    SongBiGram songPair = key.datum();
    nextCount.setSongId(songPair.getSecondSongPlayed());
    // Write out result for this song
    context.put(context.getEntityId(songPair.getFirstSongPlayed().toString()), "info" , "next_songs",
        nextCount);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return SongBiGram.SCHEMA$;
  }
}
