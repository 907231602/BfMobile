package com.bf.mobile.data.flow.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogPVRateMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.map(key, value, context);
		
		String[] values=value.toString().split("\t");
		
		String pvDates=toDate(values[1]);
		String pvIP=values[0];
		
		context.write(new Text(pvDates), new LongWritable(1));
		
		context.write(new Text(pvDates+"\t"+pvIP), new LongWritable(1));
		
		
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
