package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileReadDemo {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		System.out.println("uri: " + uri);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Reader reader = null;
		CompressionType compress = CompressionType.NONE;
		
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable keyClass = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable valueClass = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		
	}
}
