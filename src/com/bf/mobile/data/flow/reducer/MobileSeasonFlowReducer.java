package com.bf.mobile.data.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileSeasonFlowReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		int up=0;
		int down=0;
		int total=0;
		
		for (Text text : value) {
		String[] v=	text.toString().split("#");
			
			up+= Integer.parseInt(v[0]) ;
			down+= Integer.parseInt(v[1]);
		
		}
		
		total=up+down;
		
		context.write(key,new Text(up+"\t"+down+"\t"+total));
	}

}
