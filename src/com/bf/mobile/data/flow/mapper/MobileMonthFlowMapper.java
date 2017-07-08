package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileMonthFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] values=line.split("##");
		
		String phoneDate=toMonth(values[0]) ;
		String phoneNumber=values[1];
		String phoneUpFlow=values[8];
		String phoneDownFlow=values[9];
		
		//System.out.println("Mapper:"+phoneDate+"#"+phoneNumber+" flow:"+phoneUpFlow+"#"+phoneDownFlow);
		context.write(new Text(phoneDate+"#"+phoneNumber), new Text(phoneUpFlow+"#"+phoneDownFlow));

	}
	public String toMonth(String times) { 
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		return sdf.format(new Date(Long.parseLong(times)));
	}
	
}
