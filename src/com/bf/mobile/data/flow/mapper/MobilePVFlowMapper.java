package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobilePVFlowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String[] values=value.toString().split("##");
		
		String datePV=toDate(values[0]);
		String phonePV=values[1];
		
		context.write(new Text(datePV+"\t"+phonePV), new IntWritable(1));
		
	}
	
	public String toDate(String dates){
		SimpleDateFormat sd=new SimpleDateFormat("yyyy-MM-dd");
		return sd.format(new Date(Long.parseLong(dates)));
	}
}
