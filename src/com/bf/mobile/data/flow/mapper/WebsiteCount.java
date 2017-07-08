package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebsiteCount extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String values [] = value.toString().split("##");
		String phoneDate = toDate(values[0]);
		String phoneNumber = values[1];
		String webSite = values[4];
		if(webSite.equals("")){
			;
		}else{
			context.write(new Text(phoneDate+"\t"+phoneNumber+"\t"+webSite), new IntWritable(1));
		}
	}
	
	public String toDate(String times){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date(Long.parseLong(times)));
	}
}
