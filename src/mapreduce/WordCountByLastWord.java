package mapreduce;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
当前日志采样格式为
           a,b,c,d 
           b,b,f,e 
           a,a,c,f        
	    请你用最熟悉的语言编写mapreduce，计算第四列每个元素出现的个数
结果：	   d	1
	   e	1
	   f	2
	    
	    这个结果我没得出，计划用2个mapper，但是1.1.2的api不支持
 *
 */
public class WordCountByLastWord {
	
	//private static final Logger logger = Logger.getLogger(WordCountByLastWord.class) ;

	public static final String INPUT_PATH = "hdfs://crxy1:9000/temp";
	public static final String OUT_PATH = "hdfs://crxy1:9000/out";
	
	public static void main(String[] args)  throws Exception{
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
		fileSystem.delete(new Path(OUT_PATH), true);
		
		@SuppressWarnings("deprecation")
		final Job job = new Job(conf,WordCountByLastWord.class.getSimpleName());
		job.setJarByClass(WordCountByLastWord.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

//		JobConf map1Conf = new JobConf(false);  
//		ChainMapper.addMapper(job, klass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, byValue, mapperConf)
		
//		ChainMapper.addMapper(job, MyMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class, true, map1Conf);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}

public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) 
				throws java.io.IOException ,InterruptedException {
			String[] splited = value.toString().split(",");
			
			String record = splited[3];
			context.write(new Text(record), new LongWritable(0));
			for(String word: splited){
				context.write(new Text(word), new LongWritable(1));
			}
		};
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		protected void reduce(Text key, java.lang.Iterable<LongWritable> values,org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) 
				throws java.io.IOException ,InterruptedException {
			//注意：这里如果org.apache.hadoop.mapreduce.Reducer.Context context ,会看到结果不同
			
			long count = 0L;
			boolean flag = false;
			for (LongWritable times : values) {
				count += times.get();
				if(0 == times.get()) flag = true;
			}
			if(flag){
				context.write(key, new LongWritable(count));
			}
			
		};
	}
}
