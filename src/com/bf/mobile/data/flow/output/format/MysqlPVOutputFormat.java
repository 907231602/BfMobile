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

import com.bf.mobile.data.flow.dao.PVFlowDao;

public class MysqlPVOutputFormat extends OutputFormat<Text, IntWritable> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("checkOutputSpecs");
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0), arg0);
	}

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		PVFlowDao fv=new PVFlowDao();
		return new MysqlRecordWriter(fv);
	}
	
	class MysqlRecordWriter extends RecordWriter<Text, IntWritable>{

		PVFlowDao pvFlowDao;
		public MysqlRecordWriter(PVFlowDao pvFlowDao) {
			// TODO Auto-generated constructor stub
			this.pvFlowDao=pvFlowDao;
		
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(Text key, IntWritable value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] v=key.toString().split("\t");
			String dates=v[0];
			String phone=v[1];
			
			String count=Integer.toString(value.get()) ;
			
			pvFlowDao.addFlow("pv_flow", dates, phone, count);
			
			
			
		}
		
	} 

}
