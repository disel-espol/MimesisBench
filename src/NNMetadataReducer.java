package org.apache.hadoop.fs.nnmetadata;

import java.io.IOException;
import java.util.Date;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.net.InetAddress;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * Reducer class
 */
public class NNMetadataReducer extends Reducer<Text, Text, Text, Text> {
  protected String hostName;
  private static final Log LOG = LogFactory.getLog(
      NNMetadataReducer.class);

  public NNMetadataReducer () {
    LOG.info("Starting NNMetadataReducer !!!");
    try {
      hostName = java.net.InetAddress.getLocalHost().getHostName();
    } catch(Exception e) {
      hostName = "localhost";
    }
    LOG.info("Starting NNMetadataReducer on " + hostName);
  }

  /**
   * Reduce method
   */
  @Override
  public void reduce(Text key, 
      Iterable<Text> values,
      Context context
      ) throws IOException, InterruptedException {
    String field = key.toString();

    context.setStatus("starting " + field + " ::host = " + hostName);

    // sum long values
    if (field.startsWith("l:")) {
      long lSum = 0;
      for (Text val : values) {
        lSum += Long.parseLong(val.toString());
      }
      context.write(key, new Text(String.valueOf(lSum)));
    }

    if (field.startsWith("min:")) {
      long minVal = -1;
      for (Text val : values) {
        long value = Long.parseLong(val.toString());

        if (minVal == -1) {
          minVal = value;
        } else {
          if (value != 0 && value < minVal) {
            minVal = value;
          }
        }
      }
      context.write(key, new Text(String.valueOf(minVal)));
    }

    if (field.startsWith("max:")) {
      long maxVal = -1;
      for (Text val : values) {
        long value = Long.parseLong(val.toString());

        if (maxVal == -1) {
          maxVal = value;
        } else {
          if (value > maxVal) {
            maxVal = value;
          }
        }
      }
      context.write(key, new Text(String.valueOf(maxVal)));
    }

    context.setStatus("finished " + field + " ::host = " + hostName);
  }
}

