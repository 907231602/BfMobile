package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileDateFlowMapper extends
		Mapper<LongWritable, Text, Text, Text> {
		
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		String[] values=line.split("##");
		
		String phoneDate=toDate(values[0]) ;
		String phoneNumber=values[1];
		String phoneUpFlow=values[8];
		String phoneDownFlow=values[9];
		
		//System.out.println("Mapper:"+phoneDate+"#"+phoneNumber+" flow:"+phoneUpFlow+"#"+phoneDownFlow);
		context.write(new Text(phoneDate+"#"+phoneNumber), new Text(phoneUpFlow+"#"+phoneDownFlow));
	}
	
	
	public String toDate(String date){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date(Long.parseLong(date)));
	}
	
	
	public String toMonth(String times) { // 用来日志格式化
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		return sdf.format(new Date(Long.parseLong(times)));
	}
	public String getSeanson(String times) {
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
