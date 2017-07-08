package com.bf.mobile.data.flow.output.format;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.dao.LogPVRateDao;

public class MysqlLogPVRateOutPutFormat extends OutputFormat<Text, LongWritable> {

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
	public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		LogPVRateDao logPVRateDao=new LogPVRateDao();
		return new MyRecordWriter(logPVRateDao);
	}
	
	class MyRecordWriter extends RecordWriter<Text, LongWritable>{
		
		LogPVRateDao logPVRateDao;
		
		public MyRecordWriter(LogPVRateDao logPVRateDao) {
			// TODO Auto-generated constructor stub
		this.logPVRateDao=logPVRateDao;
		}

		HashMap<String, Integer> hashPV=new HashMap<String,Integer>();
		HashMap<String, Integer> hashJump=new HashMap<String,Integer>();
		
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (String iterable_element : hashJump.keySet()) {
				//System.out.println("hello");
				float jump=hashJump.get(iterable_element);
				float pv=hashPV.get(iterable_element);
				float ration=jump/pv;
				logPVRateDao.addRation(iterable_element,jump,pv,ration);
			}
			
		}

		@Override
		public void write(Text arg0, LongWritable arg1) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] value=arg0.toString().split("\t");
			
			if(value.length==2){
				int count=(int) ((hashJump.get(value[0])==null?0: hashJump.get(value[0])) +arg1.get());
				hashJump.put(value[0], count);
			}
			if(value.length==1){
				int count=(int) ((hashPV.get(arg0.toString())==null?0:hashPV.get(arg0))+arg1.get());
			    hashPV.put(arg0.toString(), count);
			}
			
		}
		
	}

}
