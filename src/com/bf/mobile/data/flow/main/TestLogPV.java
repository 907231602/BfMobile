package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.mapper.LogPVMapper;
import com.bf.mobile.data.flow.reducer.LogPVreducer;

public class TestLogPV {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		try {
			Job job=Job.getInstance();
			
			job.setMapperClass(LogPVMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(LogPVreducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/acc"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/access/PVCount"));
			
			boolean s=job.waitForCompletion(true);
			System.out.println(s);
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
