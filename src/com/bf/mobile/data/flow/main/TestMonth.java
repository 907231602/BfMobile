package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.bf.mobile.data.flow.mapper.MobileDateFlowMapper;
import com.bf.mobile.data.flow.mapper.MobileMonthFlowMapper;
import com.bf.mobile.data.flow.output.format.MysqlMonthOutPutFormat;
import com.bf.mobile.data.flow.reducer.MobileDateFlowReducer;
import com.bf.mobile.data.flow.reducer.MobileMonthFlowReducer;

public class TestMonth {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		try {
			Job job=Job.getInstance();
			job.setJarByClass(TestMonth.class);
			
			job.setMapperClass(MobileMonthFlowMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(MobileMonthFlowReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
		//	FileOutputFormat.setOutputPath(job,new Path("hdfs://yanjijun1:9000/mbs"));
			job.setOutputFormatClass(MysqlMonthOutPutFormat.class);
			boolean b= job.waitForCompletion(true);
			System.out.println(b);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
	}
	

}
