package com.bf.mobile.data.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileMonthFlowReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int upSumFlow=0;
		int downSunFlow=0;
		int totalFlow=0;
		
		for (Text v : value) {
		String[] val=v.toString().split("#");
			
			upSumFlow+=Integer.parseInt(val[0]);
			downSunFlow+=Integer.parseInt(val[1]);
		
		}
		
		totalFlow=upSumFlow+downSunFlow;
		context.write(key, new Text(upSumFlow+"\t"+downSunFlow+"\t"+totalFlow));
	}

}
