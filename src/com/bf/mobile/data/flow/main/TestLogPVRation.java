package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.bf.mobile.data.flow.mapper.LogPVRateMapper;
import com.bf.mobile.data.flow.output.format.MysqlLogPVRateOutPutFormat;
import com.bf.mobile.data.flow.reducer.LogPVRateReducer;

public class TestLogPVRation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf=new  Configuration();
		
		try {
			Job job=Job.getInstance(conf);
			job.setJarByClass(TestLogPVRation.class);
			
			job.setMapperClass(LogPVRateMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			job.setReducerClass(LogPVRateReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/acc/part-r-00000"));
			//FileOutputFormat.setOutputPath(job,new Path("hdfs://yanjijun1:9000/acc/part-r-00000"));
		   job.setOutputFormatClass(MysqlLogPVRateOutPutFormat.class);
			boolean f= job.waitForCompletion(true);
			System.out.println(f);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
