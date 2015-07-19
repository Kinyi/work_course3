package mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCounterApp {
	
	public static final String INPUT_PATH = "hdfs://crxy1:9000/hello";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, WordCountCounterApp.class.getSimpleName());
		job.setJarByClass(WordCountCounterApp.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH );
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\\s");
			Counter counter = context.getCounter("Words", "hello");
			if(value.toString().contains("hello")){
				counter.increment(1L);
			}
			for (String word : words) {
				context.write(new Text(word), new LongWritable(1));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long times = 0L;
			for (LongWritable v2 : v2s) {
				times += v2.get();
			}
			context.write(k2, new LongWritable(times));
		}
	}

}
