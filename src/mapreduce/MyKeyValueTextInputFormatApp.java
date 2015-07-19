package mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyKeyValueTextInputFormatApp {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/hello";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, MyKeyValueTextInputFormatApp.class.getSimpleName());
		job.setJarByClass(MyKeyValueTextInputFormatApp.class);
		
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable>{
		@Override
		protected void map(Text key, Text value,Context context)
				throws IOException, InterruptedException {
			context.write(key, new LongWritable(1));
			context.write(value, new LongWritable(1));
		}
	}
}
