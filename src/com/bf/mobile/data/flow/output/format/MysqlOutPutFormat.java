package com.bf.mobile.data.flow.output.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.dao.PhoneFlowDao;

public class MysqlOutPutFormat extends OutputFormat<Text, Text> {

	/**
	 * 检查是否有输出文件夹
	 */
	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 用来创建输出文件夹
	 */
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0), arg0);
	}

	/**
	 * 用来处理reducer输出记录
	 */
	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		PhoneFlowDao dao=new PhoneFlowDao();
		return new MysqlRecordWriter(dao);
	}
	
	/**
	 * 用来处理reducer输出的每一条记录
	 * @author Administrator
	 *
	 */
	class MysqlRecordWriter extends RecordWriter<Text, Text>{

		PhoneFlowDao dao;
		public MysqlRecordWriter( PhoneFlowDao phoneFlowDao) {
			// TODO Auto-generated constructor stub
			this.dao=phoneFlowDao;
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(Text key, Text value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//key
			String[] phoneKey=key.toString().split("#");
			String phoneDate=phoneKey[0];
			String phoneNumber=phoneKey[1];
			//value
			String[] phoneValue=value.toString().split("\t");
			String phoneUpFlow=phoneValue[0];
			String phoneDownFlow=phoneValue[1];
			String phoneTotalFlow=phoneValue[2];
			
			dao.addFlow(phoneDate, phoneNumber, phoneUpFlow, phoneDownFlow, phoneTotalFlow);
			
			
		}
		
	}


}
