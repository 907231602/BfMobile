package com.bf.mobile.data.flow.output.format;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mobile.data.flow.dao.LogUvDao;

public class LogUVOutputFormat extends OutputFormat<Text, LongWritable> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0),arg0);
	}

	@Override
	public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		LogUvDao dao = new LogUvDao();
		return new MysqlRecordWriter(dao);
	}
	
	class MysqlRecordWriter extends RecordWriter<Text, LongWritable>{
		LogUvDao dao;
		
		int sum = 0;
		String date;
		public MysqlRecordWriter(LogUvDao dao) {
			this.dao = dao;
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			dao.addUv(date,sum);
		}

		@Override
		public void write(Text arg0, LongWritable arg1) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			sum++;
			String s [] = arg0.toString().split("\t");
			date =s[0];
		}
		
	}

}
