package com.bf.mobile.data.flow.output.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.dao.SeasonFlowDao;

public class MysqlSeasonOutPutFormat extends OutputFormat<Text, Text> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0), arg0);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		SeasonFlowDao dao=new SeasonFlowDao();
		return new MysqlRecordWriter(dao);
	}
	
	class MysqlRecordWriter extends RecordWriter<Text, Text>{

		SeasonFlowDao dao;
		public MysqlRecordWriter( SeasonFlowDao seasonFlowDao) {
			// TODO Auto-generated constructor stub
			this.dao=seasonFlowDao;
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
			String phoneSeason=phoneKey[0];
			String phoneNumber=phoneKey[1];
			//value
			String[] phoneValue=value.toString().split("\t");
			String phoneUpFlow=phoneValue[0];
			String phoneDownFlow=phoneValue[1];
			String phoneTotalFlow=phoneValue[2];
			
			dao.addFlow(phoneSeason, phoneNumber, phoneUpFlow, phoneDownFlow, phoneTotalFlow);
			
			
		}
	}

}
