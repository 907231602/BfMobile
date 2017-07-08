package  com.bf.mobile.data.flow.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.bf.mobile.data.flow.mapper.WebsiteCount;
import com.bf.mobile.data.flow.output.format.WebsiteOutputFormat;
import com.bf.mobile.data.flow.reducer.WebsiteCountReducer;

public class TestSiteCount {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(TestSiteCount.class);
			
			job.setMapperClass(WebsiteCount.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(WebsiteCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/mobile.dat"));
			job.setOutputFormatClass(WebsiteOutputFormat.class);
			
			boolean b = job.waitForCompletion(true);
			System.out.println(b);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
