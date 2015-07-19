package old;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountApp {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/hello";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		JobConf job = new JobConf(conf, WordCountApp.class);
		job.setJarByClass(WordCountApp.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		JobClient.runJob(job);
	}
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable k1, Text v1,OutputCollector<Text, LongWritable> output, Reporter arg3)
				throws IOException {
			String[] split = v1.toString().split("\t");
			for (String word : split) {
				output.collect(new Text(word), new LongWritable(1));
			}
		}
		
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text , LongWritable>{

		@Override
		public void reduce(Text k2, Iterator<LongWritable> v2s,OutputCollector<Text, LongWritable> output, Reporter arg3)
				throws IOException {
			long times = 0L;
			while (v2s.hasNext()) {
				long v2 = ((LongWritable)v2s.next()).get();
				times += v2;
			}
			output.collect(k2, new LongWritable(times));
		}
		
	}

}
