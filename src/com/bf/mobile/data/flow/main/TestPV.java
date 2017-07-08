package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.bf.mobile.data.flow.mapper.MobilePVFlowMapper;
import com.bf.mobile.data.flow.output.format.MysqlPVOutputFormat;
import com.bf.mobile.data.flow.reducer.MobilePVFlowReducer;

public class TestPV {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			Configuration conf=new Configuration();
			
			try {
				Job job=Job.getInstance();
				job.setJarByClass(TestPV.class);
				
				job.setMapperClass(MobilePVFlowMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				
				job.setReducerClass(MobilePVFlowReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.addInputPaths(job, "hdfs://yanjijun1:9000/mobile.dat");
				job.setOutputFormatClass(MysqlPVOutputFormat.class);
				
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
