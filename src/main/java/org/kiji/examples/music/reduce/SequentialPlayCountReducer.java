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
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.examples.music.SongCount;
import org.kiji.examples.music.SongBiGram;
import org.kiji.mapreduce.AvroKeyReader;
import org.kiji.mapreduce.AvroKeyWriter;
import org.kiji.mapreduce.AvroValueWriter;
import org.kiji.mapreduce.KijiReducer;

/**
 * A reducer that will sum the values associated with a pair of songs and write that sum
 * to a column specified
 */
public class SequentialPlayCountReducer
    extends KijiReducer<AvroKey<SongBiGram>, LongWritable, AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyReader, AvroKeyWriter, AvroValueWriter {

  /** {@inheritDoc} */
  @Override
  protected void reduce(AvroKey<SongBiGram> key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    // Initialize sum to zero.
    long sum = 0L;
    // Add up all the values.
    for (LongWritable value : values) {
      sum += value.get();
    }
    // Set values for this count.
    SongCount nextSongCount = new SongCount();
    nextSongCount.setCount(sum);
    SongBiGram songPair = key.datum();
    nextSongCount.setSongId(songPair.getSecondSongPlayed());
    // Write out result for this song
    context.write(new AvroKey(songPair.getFirstSongPlayed().toString()), new AvroValue(nextSongCount));
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return SongBiGram.SCHEMA$;
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    // Programmaticlly retrieve the avro schema for a String.
    return Schema.create(Schema.Type.STRING);
  }
}
