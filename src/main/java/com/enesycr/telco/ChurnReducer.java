package com.enesycr.telco;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author enesycr on 16.03.2021
 * @project MapReduce
 */
public class ChurnReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    int count = 0;
    for (IntWritable current : values) {
      count += current.get();
    }
    context.write(key, new IntWritable(count));
  }
}
