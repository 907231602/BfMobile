package com.bf.mobile.data.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogClearMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
 
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	清洗数据
		String[] values=value.toString().split(" ");
		
		String logIP="-";
		String logTime="-";
		String loglocale="-";//时区
		String logSource="-";
		String logState="-";
		String logFlow="-";
		
		
		if(values.length>0){
			logIP=values[0];
		}
		if(values.length>3){
			logTime=values[3];
		}
		if(values.length>4){
			loglocale=values[4];
		}
		if(values.length>6){
			logSource=values[6];
		}
		if(values.length>8){
			logState=values[8];
				}
		if(values.length>9){
			logFlow=values[9];
		}
		
		
		if(logSource.startsWith("/static/image") || logSource.startsWith("/data/attachment")
				||logSource.startsWith("/data/cache")
				||logSource.startsWith("/source/plugin")
				||logSource.startsWith("/static/js")
				||logSource.startsWith("/uc_server/data")
				||logSource.startsWith("/images/smrz")
				||logSource.startsWith("/favicon.ico")
				||logSource.startsWith("/template/newdefault")
				||logSource.startsWith("/uc_server/images")
				||logSource.startsWith("/images/dxsrz")
				||logSource.startsWith("/robots.txt")
				||logSource.startsWith("/?535870")
				||logSource.startsWith("/?15811")
				||logSource.startsWith("/static//image")
				||logSource.length()<2
				||logSource.startsWith("/none") 
				||logSource.startsWith("/?28923")
				||logSource.startsWith("/?23905")
						||logSource.startsWith("/?29916")
				){
			
		}else
		{
			context.write(new Text(logIP+"\t"+logTime+" "+loglocale+"\t"+logSource+"\t"+logState+"\t"+logFlow), NullWritable.get());
		}
		
		
		
	}
}
