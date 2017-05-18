package com.sogou.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class dbnetget_cluster extends Configured implements Tool {

	private Configuration conf = null;

	  public void setConf(Configuration conf) {
		this.conf = conf;
	  }

	  public Configuration getConf() {
		return this.conf;
	  }

	  public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		System.out.println(conf.get("mapred.job.name"));
		System.out.println(conf.get("mapred.input.dir"));
		System.out.println(conf.get("mapred.output.dir"));
		//System.out.println(conf.get("libjars"));
		
		Job job = new Job(conf, conf.get("mapred.job.name"));

		job.setJarByClass(dbnetget_cluster.class);
		job.setMapperClass(dbnetget_clusterMapper.class);
		job.setReducerClass(dbnetget_clusterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//FileInputFormat.addInputPath(job, new Path(args[1]));
	    //FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if (job.waitForCompletion(true) && job.isSuccessful()) {
		  return 0;
		}
		return -1;
	  }

	  public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new dbnetget_cluster(), args);
		System.exit(res);
	  }

}
