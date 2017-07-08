package com.bf.mobile.data.flow.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogPVRateReducer extends Reducer<Text, LongWritable,Text, LongWritable> {
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.reduce(arg0, arg1, arg2);
	
		
		String[] key= arg0.toString().split("\t");
		
		
		if(key.length==2){
			long pvOut=0;
			for (LongWritable string : arg1) {
				pvOut+= string.get();
			}
			if(pvOut==1){
				arg2.write(arg0, new LongWritable(1));
			}
			
		}
		else{
			long PVcount=0;
			for (LongWritable string : arg1) {
				 PVcount+=string.get();
			}
			
			arg2.write(arg0, new LongWritable(PVcount));
			
		}
		
		
	}
}
