package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Lab 3");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 4) {
	    System.out.println("Expected parameters: <job name> <access log path> <hostname-country path> <output dir>");
	    System.exit(-1);
	} else if ("CountryRequestCount".equalsIgnoreCase(otherArgs[0])) {
		String tempOutput = otherArgs[3] + "_temp";
		Job job1 = Job.getInstance(conf, "CountryRequestCount Phase 1");
		job1.setJarByClass(CountryRequestCount.class);
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
					TextInputFormat.class, CountryRequestCount.LogMapper.class );
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[2]),
					TextInputFormat.class, CountryRequestCount.HostMapper.class ); 
	    job1.setReducerClass(CountryRequestCount.JoinReducer.class);
	    job1.setOutputKeyClass(CountryRequestCount.OUTPUT_KEY_CLASS);
	    job1.setOutputValueClass(CountryRequestCount.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job1, new Path(tempOutput));
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "CountryRequestCount Phase 2");
		String tempTempOutput = tempOutput + "_temp";
		job2.setJarByClass(CountryRequestCount.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(CountryRequestCount.SumMapper.class);
		job2.setReducerClass(CountryRequestCount.SumReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(tempOutput));
		FileOutputFormat.setOutputPath(job2, new Path(tempTempOutput));
		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(conf, "CountryRequestCount Phase 3");
		job3.setJarByClass(CountryRequestCount.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setMapperClass(CountryRequestCount.SortMapper.class);
		job3.setReducerClass(CountryRequestCount.SortReducer.class);
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setSortComparatorClass(CountryRequestCount.DecreasingComparator.class);
		FileInputFormat.addInputPath(job3, new Path(tempTempOutput));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	} else if ("CountryURLCount".equalsIgnoreCase(otherArgs[0])) {
		String tempOutput = otherArgs[3] + "_temp";
		Job job1 = Job.getInstance(conf, "CountryURLCount Phase 1");
		job1.setJarByClass(CountryURLCount.class);
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
					TextInputFormat.class, CountryURLCount.LogMapper.class );
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[2]),
					TextInputFormat.class, CountryURLCount.HostMapper.class ); 
		job1.setMapOutputValueClass(Text.class);
	    job1.setReducerClass(CountryURLCount.JoinReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileOutputFormat.setOutputPath(job1, new Path(tempOutput));
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "CountryURLCount Phase 2");
		String tempTempOutput = tempOutput + "_temp";
		job2.setJarByClass(CountryURLCount.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(CountryURLCount.SumMapper.class);
		job2.setReducerClass(CountryURLCount.SumReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(CountryURLCount.CountryUrl.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(tempOutput));
		FileOutputFormat.setOutputPath(job2, new Path(tempTempOutput));
		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(conf, "CountryURLCount Phase 3");
		job3.setJarByClass(CountryURLCount.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setMapperClass(CountryURLCount.SortMapper.class);
		job3.setReducerClass(CountryURLCount.SortReducer.class);
		job3.setMapOutputKeyClass(CountryURLCount.CountryUrl.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job3, new Path(tempTempOutput));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	} else if ("URLCountryList".equalsIgnoreCase(otherArgs[0])) {
		String tempOutput = otherArgs[3] + "_temp";
		Job job1 = Job.getInstance(conf, "URLCountryList Phase 1");
		job1.setJarByClass(URLCountryList.class);
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
					TextInputFormat.class, URLCountryList.LogMapper.class );
	    MultipleInputs.addInputPath(job1, new Path(otherArgs[2]),
					TextInputFormat.class, URLCountryList.HostMapper.class ); 
		job1.setMapOutputValueClass(Text.class);
	    job1.setReducerClass(URLCountryList.JoinReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileOutputFormat.setOutputPath(job1, new Path(tempOutput));
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "URLCountryList Phase 2");
		String tempTempOutput = tempOutput + "_temp";
		job2.setJarByClass(URLCountryList.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(URLCountryList.SortMapper.class);
		job2.setReducerClass(URLCountryList.SortReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(tempOutput));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
    System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
