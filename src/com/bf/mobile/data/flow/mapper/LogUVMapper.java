package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogUVMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		
		String[] values=value.toString().split("\t");
		
		String uvIp=values[0];
		String uvTime=toDate(values[1]);
		
		String s = "member.php?mod=register";
		if( values[2].contains(s)){
			context.write(new Text(uvTime+"\t"+s+"\t"+uvIp), new Text(uvIp));
		}
		
		
		
		 
		
	}
	
public String toDate(String ss){
		
		SimpleDateFormat in=new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss ZZZZZ]",Locale.US); 
		SimpleDateFormat out=new SimpleDateFormat("yyyy-MM-dd"); 
		Date d=null;
		try{ 
			d=in.parse(ss); //System.out.println(out.format(d)); 
			
			}catch (ParseException e){ 
				e.printStackTrace(); 
			}
		return out.format(d);
	}
}
