package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class CountryRequestCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for access logs 
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(" ");
        String output = "1,1";
        context.write(new Text(parts[0]), new Text(output));
    }
    }

    // Mapper for hostname country file
    public static class HostMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length != 2) {
            return; 
        }
        String country = parts[1].trim();
        context.write(new Text(parts[0].trim()), new Text(country + ",2"));

    }
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        String country = null;
        int count = 0;

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
                count += 1;
            }
	    }
        if (country != null && count > 0) {
            context.write(new Text(country), new IntWritable(count));
        }
	}
    } 

    public static class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            int count = Integer.parseInt(parts[1]);
            context.write(new Text(parts[0]), new IntWritable(count));
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text country, Iterable<IntWritable> count, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = count.iterator();

            while (itr.hasNext()) {
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(result, country);
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            int count = Integer.parseInt(parts[0]);
            context.write(new IntWritable(count), new Text(parts[1]));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable count, Iterable<Text> countries, Context context)
                throws IOException, InterruptedException {
            for (Text country : countries) {
                context.write(country, count);
            }
        }
    }

    public static class DecreasingComparator extends WritableComparator { 
        protected DecreasingComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1,WritableComparable w2){
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }
}
