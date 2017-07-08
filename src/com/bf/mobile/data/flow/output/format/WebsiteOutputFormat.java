package com.bf.mobile.data.flow.output.format;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.dao.WebSiteDao;

public class WebsiteOutputFormat extends OutputFormat<Text, IntWritable> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) 
			throws IOException, InterruptedException {
		
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0),arg0);
	}

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext arg0) 
			throws IOException, InterruptedException {
		WebSiteDao dao = new WebSiteDao();
		 
		return new MysqlRecordWriter(dao);
	}
	
	
	class MysqlRecordWriter extends RecordWriter<Text, IntWritable>{
		WebSiteDao webSiteDao;
		public MysqlRecordWriter(WebSiteDao webSiteDao) {
			this.webSiteDao = webSiteDao;
		}
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void write(Text key, IntWritable values) throws IOException, InterruptedException {
			String phonekye [] = key.toString().split("\t");
			
			for(String s:phonekye){
				System.out.println(s);
			}
			int value = values.get();
			
			String phoneDate = phonekye[0];
			String phoneNumber = phonekye[1];
			String webSite = phonekye[2];
			int visitCount = value;
			
			webSiteDao.addFlow(phoneDate, phoneNumber, webSite, visitCount);
		}
		
	}
}
