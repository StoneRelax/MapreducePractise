package mapreducewordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;


public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] parameterlist = args;
		if (args.length != 2){
			System.out.println(args[0].toString());
			System.out.println(args[1].toString());
			System.err.println("Usage : [input] [output]");
		}
		Job job = new Job(conf,"test word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setCombinerClass(CountReducer.class);
		job.setNumReduceTasks(1);
		TextInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
	
	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>{
		Text txt = new Text();
		IntWritable count = new IntWritable(1);

		public void map(Object key , Text value , Context context) throws IOException, InterruptedException{
			StringTokenizer line = new StringTokenizer(value.toString());
			while (line.hasMoreTokens()){
				txt.set(line.nextToken());
				context.write(txt,count);
			}
		}
	}
	
	public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		IntWritable totalcount = new IntWritable(0);
		public void reduce (Text key , Iterable<IntWritable> values , Context context) throws IOException, InterruptedException{
			int countnum = 0 ;
			for (IntWritable count : values){
				countnum += count.get();
				totalcount.set(countnum);
			}
			context.write(key,totalcount);
		}
	}
	
	
}
