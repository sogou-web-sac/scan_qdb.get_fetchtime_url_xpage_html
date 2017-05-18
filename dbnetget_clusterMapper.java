package com.sogou.cluster;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class dbnetget_clusterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	private IntWritable _rand_queue = new IntWritable();
	private Text _url  = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException,
	  InterruptedException {
	  // preconditions :
	  // value must be a url
	
	  String url = value.toString();
	  context.getCounter("MapModel", "line_number").increment(1);
	  
	  Random random = new Random(System.currentTimeMillis());
	  int r = random.nextInt(300)+1; // [1, 300]
	  
	  _rand_queue.set(r);
	  _url.set(url);
	  context.write(_rand_queue, _url);
	  
	}

}
