package com.bf.mobile.data.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogUVReducer extends Reducer<Text, Text, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		String[] keys= key.toString().split("\t");
		
		context.write(new Text(keys[0]+"\t"+keys[2]), new LongWritable(1));
	}
}
