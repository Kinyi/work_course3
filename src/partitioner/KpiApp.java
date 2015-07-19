package partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KpiApp {
	
	public static final String INPUT_PATH = "hdfs://crxy1:9000/in";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, KpiApp.class.getSimpleName());
		job.setJarByClass(KpiApp.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		@Override
		protected void map(LongWritable k1, Text v1,Context context)
				throws IOException, InterruptedException {
			String[] fields = v1.toString().split("\t");
			String mobileNum = fields[1];
			Text k2 = new Text(mobileNum);
			KpiWritable v2 = new KpiWritable(Long.parseLong(fields[6]), Long.parseLong(fields[7]), Long.parseLong(fields[8]), Long.parseLong(fields[9]));
			context.write(k2, v2);
		}
	}
	
	public static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		@Override
		protected void reduce(Text k2, Iterable<KpiWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long upPackage = 0L ;
			long downPackage = 0L ;
			long upPayLoad = 0L ;
			long downPayLoad = 0L ;
			
			for (KpiWritable v2 : v2s) {
				upPackage += v2.upPackage;
				downPackage += v2.downPackage;
				upPayLoad += v2.upPayLoad;
				downPayLoad += v2.downPayLoad;
			}
			
			KpiWritable v3 = new KpiWritable(upPackage, downPackage, upPayLoad, downPayLoad);
			context.write(k2, v3);
		}
	}
	
	public static class MyPartitioner extends Partitioner<Text, KpiWritable>{

		@Override
		public int getPartition(Text k2, KpiWritable v2, int numPartitions) {
			int length = k2.toString().length();
			return length == 11?0:1;
		}
		
	}
}

class KpiWritable implements Writable{
	
	long upPackage;
	long downPackage;
	long upPayLoad;
	long downPayLoad;
	
	public KpiWritable(){}
	
	public KpiWritable(long upPackage, long downPackage, long upPayLoad,
			long downPayLoad) {
		super();
		this.upPackage = upPackage;
		this.downPackage = downPackage;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackage);
		out.writeLong(downPackage);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackage = in.readLong();
		this.downPackage = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public String toString() {
		return upPackage + "\t"+ downPackage + "\t" + upPayLoad + "\t"+ downPayLoad;
	}
	
}

