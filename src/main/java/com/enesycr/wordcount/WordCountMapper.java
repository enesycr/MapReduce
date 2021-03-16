package com.enesycr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author enesycr on 3/16/21
 * @project MapReduce
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public static final IntWritable ONE = new IntWritable(1);

  @Override
  protected void map(LongWritable offset, Text line, Context context)
      throws IOException, InterruptedException {
    for (String word : line.toString().split(" ")) {
      context.write(new Text(word), ONE);
    }
  }
}
