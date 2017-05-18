package com.sogou.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import sun.misc.BASE64Encoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sogou.pages.OffsumPagesDumper;

public class dbnetget_clusterReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

	public String filter_control_char(String s) {
		return s.replace("\n", "").replace("\r", "").replace("\t", "");
	}
		
	public String get_fetch_time(String s) throws ParseException {
		String s_begin = "Fetch-Time:";
		String s_end   = "\n";
		int b = s.indexOf(s_begin);
		if (b == -1) return "5000000000";
		int e = s.indexOf(s_end, b);
		if (e == -1) return "5000000000";
		String fetch_time = s.substring(b+s_begin.length(), e).trim();
		
		SimpleDateFormat parser = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", Locale.ENGLISH);
		
		Date date = parser.parse(fetch_time);
		Long ts = date.getTime()/1000;
		return ts.toString();
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		// key is the server ID of database
		int server_id = Integer.parseInt(key.toString());
		String server_ip_format = context.getConfiguration().get("offsum.server.ip.format"); // like "offsum%03d.web.nm.ted"
		String host = String.format(server_ip_format, server_id);
		int port = Integer.parseInt( context.getConfiguration().get("offsum.server.port") );
		
		OffsumPagesDumper dumper = new OffsumPagesDumper();	
		dumper.Init(host, port);
	  
		for (Text v : values) {
			try {
				context.getCounter("ReduceModel", "The # Of URL").increment(1);
				String url = v.toString();
				byte[] data = dumper.getPage(url, 2);
				String content = new String(data, StandardCharsets.UTF_8);
				
				String fetch_time = get_fetch_time(content);
				String xpage = filter_control_char( dumper.getXpage(url, 2) );
				String html = filter_control_char( dumper.getHtmlSource(url, 2) );
			
				String base64_xpage = filter_control_char(new BASE64Encoder().encode(xpage.getBytes("UTF-8")) );
				String base64_html  = filter_control_char(new BASE64Encoder().encode(html.getBytes("UTF-8")) );
				
				String output = String.format("%s\t%s\t%s\t%s", fetch_time, url, base64_xpage, base64_html);
				context.write(new Text(output), NullWritable.get());
			} catch (Exception e) {
				context.getCounter("ReduceModel", "Exception").increment(1);
			}
		}
		
	}
	
}
