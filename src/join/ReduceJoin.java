package join;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * reduce端join
-------file1[ID	NAME]--------  
1	zhangsan
2	lisi
3	wangwu

-------file2[ID	VALUE]-------
1	45
2	56
3	89

-------结果[NAME VALUE]--------
zhagnsan	45
lisi		56
wangwu		89
 *
 */
public class ReduceJoin {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/file";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, ReduceJoin.class.getSimpleName());
		job.setJarByClass(ReduceJoin.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}

	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String v2 = null;
			String[] split = value.toString().split("\t");
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().toString();
			if(path.contains("file1")){
				v2 = "#"+split[1];
			}
			if(path.contains("file2")){
				v2 = "*"+split[1];
			}
			context.write(new LongWritable(Long.parseLong(split[0])), new Text(v2));
		}
	}
	
	//利用标记来区分哪个是key，哪个是value
	public static class MyReducer extends Reducer<LongWritable, Text, Text, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			String k3 = null;
			String v3 = null;
			for (Text v2 : v2s) {
				String temp = v2.toString();
				if(temp.contains("#")){
					k3 = temp;
				}else if(temp.contains("*")){
					v3 = temp;
				}
			}
			context.write(new Text(k3.substring(1)), new Text(v3.substring(1)));
		}
	}
}
