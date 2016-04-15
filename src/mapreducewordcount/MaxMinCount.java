package mapreducewordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.record.compiler.JBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;


public class MaxMinCount {

	public static void main(String[] args) throws IOException {
		Configuration config = new Configuration();
		Job job = new Job(config,"Max Min Count");
		job.setJarByClass(MaxMinCount.class);
		job.setMapperClass(MMCMapper.class);	
		job.setReducerClass(MMCReducer.class);
		job.setCombinerClass(MMCReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TripleResult.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TripleResult.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path(args[0]));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job,new Path(args[1]));
		boolean rc = false ;
		try {
			rc = job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (rc){
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
	
	public static class MMCMapper extends Mapper<Object,Text,Text,TripleResult> {


		protected TripleResult result = new TripleResult();
		Text outputkey = new Text();
		@Override
		protected void map(Object key , Text value , Context context) throws IOException, InterruptedException{
			
			String str = new String(value.getBytes());
			String[] newstr = str.split("\t");

			result.setMax(Integer.parseInt(newstr[0]));
			result.setMin(Integer.parseInt(newstr[0]));
			result.setCount(1);
			outputkey.set(newstr[1]);
			context.write(outputkey,result);
		}
	}
	
	public static class MMCReducer extends Reducer<Text,TripleResult,Text,TripleResult>{
		TripleResult result = new TripleResult();
		@Override
		protected void reduce(Text key ,Iterable<TripleResult> values , Context context) throws IOException, InterruptedException{
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			int count = 0;
			
			for (TripleResult record : values){
				if (record.getMax() > max){
				max = record.getMax();
			}
			if (record.getMin() < min){
				min = record.getMin() ;
			}
			count = count + record.getCount();
			}
			result.setMax(max);
			result.setMin(min);
			result.setCount(count);
			context.write(key,result);
		}
	}
	

		
	
}
