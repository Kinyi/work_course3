package cmd;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountApp extends Configured implements Tool{
	public static String INPUT_PATH = null;
	public static String OUT_PATH = null;

	@Override
	public int run(String[] args) throws Exception {
		INPUT_PATH = args[0];
		OUT_PATH = args[1];
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountApp(), args);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\\s");
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
