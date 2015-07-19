package mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyGenericWritableApp {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/files";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, MyGenericWritableApp.class.getSimpleName());
		job.setJarByClass(MyGenericWritableApp.class);
		
		MultipleInputs.addInputPath(job, new Path("hdfs://crxy1:9000/files/hello"), 
				KeyValueTextInputFormat.class, MyMapper.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://crxy1:9000/files/hello2"), 
				TextInputFormat.class, MyMapper2.class);
		
		//job.setMapperClass(MyMapper.class);	//不应该有这一行，因为有两个map函数
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyGenericWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text, MyGenericWritable>{
		@Override
		protected void map(Text key, Text value,Context context)
				throws IOException, InterruptedException {
			context.write(key, new MyGenericWritable(new LongWritable(1)));
			context.write(value, new MyGenericWritable(new LongWritable(1)));
		}
	}
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, MyGenericWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			for (String word : split) {
				context.write(new Text(word), new MyGenericWritable(new Text("1")));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, MyGenericWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<MyGenericWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long times = 0L;
			for (MyGenericWritable myGenericWritable : v2s) {
				Writable writable = myGenericWritable.get();
				if(writable instanceof LongWritable){
					times += ((LongWritable)writable).get();
				}
				if(writable instanceof Text){
					times += Long.parseLong(((Text)writable).toString());
				}
			}
			context.write(k2, new LongWritable(times));
		}
	}
	
	public static class MyGenericWritable extends GenericWritable{
		
		public MyGenericWritable(){}

		public MyGenericWritable(LongWritable longWritable) {
			super.set(longWritable);
		}

		public MyGenericWritable(Text text) {
			super.set(text);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Class<? extends Writable>[] getTypes() {
			return new Class[]{LongWritable.class,Text.class};
		}
	}
}
