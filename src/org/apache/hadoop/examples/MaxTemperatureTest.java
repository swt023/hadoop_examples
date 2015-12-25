package org.apache.hadoop.examples;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.MaxTemperature.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.junit.*;

public class MaxTemperatureTest {
	@Test
	public void processValidRecord() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
				"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		MaxTemperatureMapper.Context context = mock(MaxTemperatureMapper.Context.class);
		
		mapper.map(null, value, context);
		System.out.println("map test begin!");
		verify(context).write(new Text("1950"), new IntWritable(-11));
		System.out.println("map test end!");
	}
	
	@Test
	public void processReduce() throws IOException, InterruptedException {
		MaxTemperatureReducer reducer = new MaxTemperatureReducer();
		Text key = new Text("1950");
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(10));
		values.add(new IntWritable(20));
		
		MaxTemperatureReducer.Context context = mock(MaxTemperatureReducer.Context.class);
		reducer.reduce(key, values, context);
		
		System.out.println("reduce test begin!");
		verify(context).write(new Text("1950"), new IntWritable(20));
		System.out.println("reduce test end!");
	}
	
	@Test
	public void testDriver() throws Exception {
		System.out.println("driver test begin!");
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "file:///");
		//conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("input");
		Path output = new Path("output");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {input.toString(), output.toString()});
		assertEquals(exitCode, 0);
		
		System.out.println("driver test end!");

	}
}
