package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class URLCountryList {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for access logs 
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(" ");
        String output = parts[6] + ",1";
        // output: <hostname, "url,1">
        context.write(new Text(parts[0]), new Text(output));
    }
    }

    // Mapper for hostname_country file
    public static class HostMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length != 2) {
            return; 
        }
        // output: <hostname, "country,2">
        String country = parts[1].trim();
        context.write(new Text(parts[0].trim()), new Text(country + ",2"));

    }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        String country = null;
        String url = null;

	    for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length != 2) {
                continue;
            }
            String source = parts[1].trim();

            // split by source
            if (source.equals("2")) {
                country = parts[0];
            } else if (source.equals("1")) {
                url = parts[0];
            }
	    }

        if (country != null && url != null) {
            context.write(new Text(url), new Text(country));
        }
	}
    } 

    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text url, Iterable<Text> countries, Context context)
                throws IOException, InterruptedException {        
            TreeSet<String> uniqueCountries = new TreeSet<>();

            for (Text country : countries) {
                uniqueCountries.add(country.toString());
            }

            String countryList = String.join(", ", uniqueCountries);
            context.write(url, new Text(countryList));
        }
    }

}


