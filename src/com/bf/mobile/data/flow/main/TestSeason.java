package com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.bf.mobile.data.flow.mapper.MobileSeasonFlowMapper;
import com.bf.mobile.data.flow.output.format.MysqlSeasonOutPutFormat;
import com.bf.mobile.data.flow.reducer.MobileSeasonFlowReducer;

public class TestSeason {
	public static void main(String[] args) {
		Configuration con= new Configuration();
		
		try {
			Job job=Job.getInstance();
			job.setJarByClass(TestSeason.class);
			
			job.setMapperClass(MobileSeasonFlowMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(MobileSeasonFlowReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
			
			job.setOutputFormatClass(MysqlSeasonOutPutFormat.class);
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
