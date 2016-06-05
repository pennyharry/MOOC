/**
 * @description: 大数据系统基础课程实验1，计算用户发的微博数目，并按照从大到小排列
 * @author     : harrypenny
 * @time       : 2016-5-16
 */


import java.io.IOException;
import java.util.Random;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class UserCount {
	public static class CountMapper extends Mapper<Object, Text, Text, Text>
	{	     

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{  
	        //implement here
	    	String id = null;
	    	String name = null;
	    	try {
				JSONObject json = new JSONObject(value.toString());
				// 用户发的微博数据
				if (json.has("user_id") && json.has("text")){
					id = json.optString("user_id");
					context.write(new Text(id), new Text("1"));
//					System.out.println("id = " + id + " cnt = 1");
				}else{
					// 用户name数据
					if(json.has("_id") && json.has("name")){
						id = json.optString("_id");
						name = json.optString("name");
						context.write(new Text(id), new Text("name$" + name));
//						System.out.println("id = " + id + " name = " + name);
					}
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}

	public static class CountReducer extends Reducer<Text,Text,Text,IntWritable> 
	{
		// 判断一个字符串是否全部由数字字符组成，即是否是一个数字
		public boolean isNumbers(String s){
			Pattern p = Pattern.compile("\\d+");
			Matcher isNumber = p.matcher(s);
			if (!isNumber.matches()){
				return false;
			}
			return true;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			//implement here
			int count = 0;           // 用户发的微博数目
			String name = null;      // 用户name
			String tmp = null;
			for (Text t : values){
				tmp = t.toString();
				// 当前数据为用户name,则将其name保存
				if (tmp.startsWith("name$")){
					name = tmp.substring(5);
				}else if (isNumbers(tmp) &&   // 当前数据是用户发的微博，则累加用户发的微博数目
					Integer.valueOf(tmp) < Integer.MAX_VALUE){
						count += Integer.valueOf(tmp);
				}
			}
			// 用户id有对应的name
			if (name != null){
			    context.write(new Text(name), new IntWritable(count));
			}
//			System.out.println("name = " + name + " count = " + count);
		}
	}
	
	public static class SortMapper extends Mapper<Object, Text, IntWritable,Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//implement here
			String []tmp = value.toString().split("\t");
			// 将（用户name，微博条数）转化为（微博条数，用户name）
			if (tmp.length >= 2){
				context.write(new IntWritable(Integer.valueOf(tmp[1])), new Text(tmp[0]));
			}
		}
	}

	public static class SortReducer extends Reducer<IntWritable,Text,IntWritable,Text> 
	{
		
		public void reduce(IntWritable key,Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			//implement here
			// 输出最后结果，（微博条数，用户name）
			for (Text t : values){
				context.write(key, t);
//				System.out.println("key = " + key.get() + " value = " + t.toString());
			}
		}
	}
	


	private static class IntDecreasingComparator extends IntWritable.Comparator 
	{
		//重写IntWritable.Comparator的compare来实现自定义的从大到小排序
		public int compare(WritableComparable a, WritableComparable b) 
		{
			//implement here
			return -1 * super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			//implement here
			return -1 * super.compare(b1, s1, l1, b2, s2, l2);
		}
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NameCount-count");
		
		job.setJarByClass(UserCount.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/input-user"));
		Path tempDir = new Path("temp");
		FileOutputFormat.setOutputPath(job, tempDir);
		
		job.waitForCompletion(true); 
		
		//implement here
		//在SortJob中使用“sortJob.setSortComparatorClass(IntDecreasingComparator.class)”
		//来把你的输出排序方式设置为你自己写的IntDecreasingComparator
		Configuration conf2 = new Configuration();
		Job sortJob = new Job(conf2, "sort-count");
		
		sortJob.setJarByClass(UserCount.class);
		sortJob.setMapperClass(SortMapper.class);
		sortJob.setReducerClass(SortReducer.class);
		
		sortJob.setMapOutputKeyClass(IntWritable.class);  
		sortJob.setMapOutputValueClass(Text.class);
		
		sortJob.setSortComparatorClass(IntDecreasingComparator.class);

		sortJob.setOutputKeyClass(IntWritable.class);
		sortJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(sortJob, tempDir);
		FileOutputFormat.setOutputPath(sortJob, new Path("/output-user"));
		sortJob.waitForCompletion(true);
		
		System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
	}
}
