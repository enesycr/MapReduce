package com.enesycr.telco;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author enesycr on 16.03.2021
 * @project MapReduce
 */
public class ChurnMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  // header
  // customerID,gender,SeniorCitizen,Partner,Dependents,tenure,PhoneService,MultipleLines,InternetService,OnlineSecurity,OnlineBackup,DeviceProtection,TechSupport,StreamingTV,StreamingMovies,Contract,PaperlessBilling,PaymentMethod,MonthlyCharges,TotalCharges,Churn
  // find gender churn distribution


  public static final IntWritable ONE = new IntWritable(1);

  @Override
  protected void map(LongWritable offset, Text line, Mapper.Context context) throws IOException, InterruptedException {
    String[] columnValues = line.toString().split(",");

    String genderChurn = columnValues[1] + ", " + columnValues[20];
    for (String word : line.toString().split(",")) {
      context.write(new Text(genderChurn), ONE);
    }
  }
}
