package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.mapper.LogClearMapper;

public class TestLog {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			Configuration conf=new  Configuration();
			
			try {
				Job job=Job.getInstance();
				job.setJarByClass(TestLog.class);
				
				job.setMapperClass(LogClearMapper.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				
				
				FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/access.log"));
				FileOutputFormat.setOutputPath(job,new Path("hdfs://yanjijun1:9000/acc"));
			
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
