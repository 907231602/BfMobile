package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.mapper.MobileDateFlowMapper;
import com.bf.mobile.data.flow.output.format.MysqlOutPutFormat;
import com.bf.mobile.data.flow.reducer.MobileDateFlowReducer;


public class TestMobile {
	public static void main(String[] args) {
		Configuration conf=new Configuration();	
		try {
			Job job=Job.getInstance();
			job.setJarByClass(TestMobile.class);
			
			job.setMapperClass(MobileDateFlowMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(MobileDateFlowReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		//	FileOutputFormat.setOutputPath(job,new Path("hdfs://yanjijun1:9000/mbs"));
			job.setOutputFormatClass(MysqlOutPutFormat.class);
			boolean b= job.waitForCompletion(true);
			System.out.println(b);
			
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
