package mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//没有能实现出来
public class WordCountByLastWord2 {
	static ArrayList<Integer> list = new ArrayList<Integer>();
	public static final String INPUT_PATH = "hdfs://crxy1:9000/temp";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, WordCountByLastWord2.class.getSimpleName());
		job.setJarByClass(WordCountByLastWord2.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			for (String word : split) {
				context.write(new Text(word), new LongWritable(1));
			}
			int code = (int)(split[3].charAt(0));
			WordCountByLastWord2.list.add(code);
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long times = 0L;
			int code = k2.toString().charAt(0);
			if(WordCountByLastWord2.list.contains(code)){
				for (LongWritable time : v2s) {
					times += time.get();
				}
				context.write(new Text(k2), new LongWritable(times));
			}
		}
	}

}
