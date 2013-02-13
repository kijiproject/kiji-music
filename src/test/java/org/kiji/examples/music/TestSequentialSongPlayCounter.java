// (c) Copyright 2012 WibiData, Inc.

package org.kiji.examples.music;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

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
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
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
  private KijiURI mSongTableURI;
  private KijiTable mSongTable;
  private KijiTableReader mSongTableReader;

  @Before
  public final void setup() throws Exception {
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();
    final KijiTableLayout songLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/songs.json");
    final String songTableName = songLayout.getName();
    mSongTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(songTableName).build();


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
        .withTable(songLayout.getName(),songLayout)
        .build();
    mSongTable = getKiji().openTable(songTableName);
    mSongTableReader = mSongTable.openTableReader();
  }

  @After
  public final void teardown() throws Exception {
    mSongTableReader.close();
    mSongTable.close();
  }

  @Test
  public void testSongPlayCounter() throws Exception {
    final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
    MapReduceJobOutput tableOutput = new DirectKijiTableMapReduceJobOutput(mSongTableURI, 1);

    final MapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(SequentialPlayCounter.class)
        .withReducer(SequentialPlayCountReducer.class)
        .withInputTable(mUserTableURI)
        // Note: the local map/reduce job runner does not allow more than one reducer:
        .withOutput(tableOutput)
        .build();
    assertTrue(mrjob.run());
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(Integer.MAX_VALUE).add("info", "next_songs");
    KijiDataRequest request = builder.build();
    NextSongCount valueForSong1 = mSongTableReader
      .get(mSongTable.getEntityId("song-1"), request).getMostRecentValue("info", "next_songs");
    assertEquals("Unexpected song played after song-1: ","song-2", valueForSong1.getSongId()
        .toString());
    assertEquals("Unexpected number of plays for song-2 after song-1: ", 2,
        valueForSong1.getCount().intValue());
    NextSongCount valueForSong2 = mSongTableReader
      .get(mSongTable.getEntityId("song-2"), request).getMostRecentValue("info", "next_songs");
    assertEquals("Unexpected song played after song-2: ","song-3", valueForSong2.getSongId()
        .toString());
    assertEquals("Unexpected number of plays for song-2 after song-1: ", 1,
        valueForSong2.getCount().intValue());
  }
}
