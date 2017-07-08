package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileSeasonFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		String[] values=line.split("##");
		
		String phoneSeason=toSeanson(values[0]) ;
		String phoneNumber=values[1];
		String phoneUpFlow=values[8];
		String phoneDownFlow=values[9];
		
		//System.out.println("Mapper:"+phoneDate+"#"+phoneNumber+" flow:"+phoneUpFlow+"#"+phoneDownFlow);
		context.write(new Text(phoneSeason+"#"+phoneNumber), new Text(phoneUpFlow+"#"+phoneDownFlow));
	}
	
	
	public String toSeanson(String times) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(Long.parseLong(times.trim()));		
		int month = calendar.get(Calendar.MONTH) + 1;
		int season = 0;
		if (month % 3 == 0) {
			season = month / 3;
		} else {
			season = month / 3 + 1;
		}
		
		int year = calendar.get(Calendar.YEAR) + 1;
		return year+"-"+season;
	}
}
