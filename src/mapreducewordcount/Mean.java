package mapreducewordcount;

import java.io.IOException;




import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;



public class Mean {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job =  new Job(conf,"Mean");
		job.setJarByClass(Mean.class);
		job.setMapperClass(MeanMapper.class);
		job.setReducerClass(MeanReducer.class);
		job.setCombinerClass(MeanReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MeanTriple.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path(args[0]));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job,new Path(args[1]));
		int exitcode = job.waitForCompletion(true)?0:1;
		System.exit(exitcode);
	}
	public static class MeanMapper extends Mapper<Object,Text,Text,MeanTriple>{
		MeanTriple valueresult = new MeanTriple();
		Text outputkey = new Text();
		@Override
		protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String str = new String(value.getBytes());
			String[] newstr = str.split("\t");
			int total = Integer.parseInt(newstr[0]);
			int count = 1;
			valueresult.setTotal(total);
			valueresult.setCount(count);
			valueresult.setAverage(total);

			outputkey.set(newstr[1]);
			context.write(outputkey,valueresult);
		}
	}
	
	public static class MeanReducer extends Reducer<Text,MeanTriple,Text,MeanTriple>{
		MeanTriple result = new MeanTriple();
		Text outputkey = new Text();
		@Override
		protected void reduce(Text key , Iterable<MeanTriple> values ,Context context) throws IOException, InterruptedException{
			int total = 0;
			int count = 0;
			for (MeanTriple record : values){
				total = total + record.getTotal();
				count = count + record.getCount();
			}
			outputkey = key ;
			result.setTotal(total);
			result.setCount(count);
			result.setAverage((float)total/count);
			context.write(outputkey,result);
		}
		
	}
}
