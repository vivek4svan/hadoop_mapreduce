package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class WordCountDriver {
	public static void main(String [] args) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration c = new Configuration();
		Job j = new Job(c, "wordcount");
		j.setJarByClass(WordCountDriver.class);
		j.setMapperClass(WordCountMapper.class);
		j.setReducerClass(WordCountReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
		System.out.println("Execution Completed");
	}
}
