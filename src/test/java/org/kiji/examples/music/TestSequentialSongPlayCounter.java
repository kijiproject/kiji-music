// (c) Copyright 2012 WibiData, Inc.

package org.kiji.examples.music;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.gather.SequentialPlayCounter;
import org.kiji.examples.music.reduce.SequentialPlayCountReducer;
import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Test for SequentialPlayCounter. */
public class TestSequentialSongPlayCounter extends KijiClientTest {
   private static final Logger LOG = LoggerFactory.getLogger(TestSongPlayCounter.class);

  private KijiURI mUserTableURI;

  @Before
  public final void setup() throws Exception {
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();


    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-3")
                .withValue(3L, "song-2")
                .withValue(4L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-5")
        .build();
  }

  @Test
  public void testSongPlayCounter() throws Exception {
    final File outputDir = new File(getLocalTempDir(), "output.sequence_file");

    final MapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(SequentialPlayCounter.class)
        .withReducer(SequentialPlayCountReducer.class)
        .withInputTable(mUserTableURI)
        // Note: the local map/reduce job runner does not allow more than one reducer:
        .withOutput(new AvroKeyValueMapReduceJobOutput(new Path("file://" + outputDir), 1))
        .build();
    assertTrue(mrjob.run());

    final Map<String, SongCount> counts = Maps.newTreeMap();
    readAvroKVFile(new File(outputDir, "part-r-00000"), counts);
    LOG.info("Counts map: {}", counts);
    assertEquals(2, counts.size());
  }

   /**
   * Reads a sequence file of (song ID, # of song plays) into a map.
   *
   * @param path Path of the sequence file to read.
   * @param map Map to fill in with (song ID, # of song plays) entries.
   * @throws Exception on I/O error.
   */
  private void readAvroKVFile(File file, Map<String, SongCount> map) throws Exception {
    Path path = new Path("file://" + file.toString());
    SeekableInput input = new FsInput(path, getConf());
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);

    for (GenericRecord kvPairs : fileReader) {
      String song = (String) kvPairs.get("key");
      LOG.info("the class of knPairs.get(value) is :" + kvPairs.get("value").getClass().toString());
    }
    fileReader.close();
  }
}
