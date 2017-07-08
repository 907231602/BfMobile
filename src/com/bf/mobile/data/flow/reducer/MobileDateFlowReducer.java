package com.bf.mobile.data.flow.reducer;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileDateFlowReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
			
		int upSum=0;
		int downSun=0;
		int totalSum=0;
		System.out.println("key: "+key.toString()+" Reducer values:"+values.toString());
		System.out.println(new Date());
		for (Text value : values) {
				String[] flows=value.toString().split("#");
				String upFlow=flows[0];
				String downFlow=flows[1];
				upSum+=Integer.parseInt(upFlow);
				downSun+=Integer.parseInt(downFlow);
			}
		
		totalSum =upSum+downSun;
		arg2.write(key, new Text(upSum+"\t"+downSun+"\t"+totalSum));
		
	}
}
