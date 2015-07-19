package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *3	 3   1	1
 *3	 2	 2	1
 *3	 1	 2	2
 *2	 2	 3	1
 *2	 1	 3 	2
 *1	 1 	 3	3
 */
public class SortApp {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/data";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, SortApp.class.getSimpleName());
		job.setJarByClass(SortApp.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, NewK2, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new NewK2(Long.parseLong(split[0]), Long.parseLong(split[1])), NullWritable.get());
		}
	}
	
	public static class MyReducer extends Reducer<NewK2, NullWritable, LongWritable, LongWritable>{
		protected void reduce(NewK2 k2, java.lang.Iterable<NullWritable> v2s, 
				org.apache.hadoop.mapreduce.Reducer<NewK2,NullWritable,LongWritable,LongWritable>.Context context) 
						throws IOException ,InterruptedException {
			context.write(new LongWritable(k2.first), new LongWritable(k2.second));
		};
	}
	
	public static class NewK2 implements WritableComparable<NewK2>{
		long first;
		long second;
		
		public NewK2() {}

		public NewK2(long first, long second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public int compareTo(NewK2 o) {
			long minus = this.first - o.first;
			if(minus != 0){
				return (int)minus;
			}
			return (int)(this.second-o.second);
		}
	}
}
