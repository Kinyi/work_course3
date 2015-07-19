package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyGenericWritableApp2 {
	/**
	 * user表的数据结构
	 * 1	Allen
	 * 2	martin
	 * 3	Bryant
	 * trade文件格式
	 * 1	12
	 * 2	24
	 * 2	12
	 * 3	45
	 * 目标结果
	 * Allen	12
	 * martin	36
	 * Bryant	45
	 */
	public static final String OUT_PATH = "hdfs://hadoop0:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop0:3306/test", "root", "admin");
		
		FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, MyGenericWritableApp2.class.getSimpleName());
		job.setJarByClass(MyGenericWritableApp2.class);
		DBInputFormat.setInput(job, MyUser.class, "user", null, null, "id" , "name");
		
		MultipleInputs.addInputPath(job, new Path("hdfs://hadoop0:9000/"), DBInputFormat.class, MyMapper.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://hadoop0:9000/trade"), TextInputFormat.class, MyMapper2.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyGenericWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, MyUser, Text, MyGenericWritable>{
		@Override
		protected void map(LongWritable key, MyUser value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), new MyGenericWritable(new Text(split[1])));
		}
	}
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, MyGenericWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			long v2 = Long.parseLong(split[1]); 
			context.write(new Text(split[0]), new MyGenericWritable(new LongWritable(v2)));
		}
	}
	
	public static class MyReducer extends Reducer<Text, MyGenericWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text k2, Iterable<MyGenericWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long num = 0L;
			String result = "";
			String process = "";
			for (MyGenericWritable object : v2s) {
				Writable writable = object.get();
				if(writable instanceof LongWritable){
					num += ((LongWritable) writable).get();
				}
				if(writable instanceof Text){
					process = ((Text)writable).toString();
				}
			}
			result = process+"\t"+num;
			context.write(new Text(result), NullWritable.get());
		}
	}
	
	public static class MyUser implements Writable,DBWritable{
		int id;
		String name;

		@Override
		public void readFields(ResultSet arg0) throws SQLException {
			this.id = arg0.getInt(1);
			this.name = arg0.getString(2);
		}

		@Override
		public void write(PreparedStatement arg0) throws SQLException {
			arg0.setInt(1, id);
			arg0.setString(2, name);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			Text.writeString(out, name);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
		}

		@Override
		public String toString() {
			return id + "\t" + name;
		}
	}
	
	public static class MyGenericWritable extends GenericWritable{
		public MyGenericWritable(){}

		public MyGenericWritable(Text text) {
			super.set(text);
		}

		public MyGenericWritable(LongWritable longWritable) {
			super.set(longWritable);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Class<? extends Writable>[] getTypes() {
			return new Class[]{Text.class,LongWritable.class};
		}
	}
}
