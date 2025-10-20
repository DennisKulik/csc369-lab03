package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
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


public class CountryURLCount {

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
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

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
            // group country and url
            String newKey = country + "," + url;
            context.write(new Text(newKey), new IntWritable(1));
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

    public static class SumReducer extends Reducer<Text, IntWritable, CountryUrl, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> itr = values.iterator();
            // key is "country,url", split into country and url
            String[] parts = key.toString().split(",", 2);

            int sum = 0;
            while (itr.hasNext()) {
                sum += itr.next().get();
            }

            CountryUrl customKey = new CountryUrl(new Text(parts[0]), new IntWritable(sum));
            context.write(customKey, new Text(parts[1]));

            // String customString = parts[0] + ":" + sum;
            // context.write(new Text(customString), new Text(parts[1]));
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, CountryUrl, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // input: "country,url\tcount"

            // this isnt detecting the tabs for some reason so its not splitting right
            String one = value.toString();
            String[] parts = one.split("\t");
            
            String[] countryUrl = parts[0].split(",", 2);
            int count = Integer.parseInt(parts[1]);

            context.write(new CountryUrl(new Text(countryUrl[0]), new IntWritable(count)), new Text(countryUrl[1]));
        }
    }

    public static class SortReducer extends Reducer<CountryUrl, Text, Text, IntWritable> {
        @Override
        protected void reduce(CountryUrl key, Iterable<Text> urls, Context context)
                throws IOException, InterruptedException {
            for (Text url : urls) {
                Text outputKey = new Text(key.country.toString() + "\t" + url.toString());
                context.write(outputKey, key.count);
            }
        }
    }
    
    public static class CountryUrl implements WritableComparable<CountryUrl> {
        Text country;
        IntWritable count;

        public CountryUrl(Text country, IntWritable count) {
            this.country = new Text(country);
            this.count = new IntWritable(count.get());
        }
        public CountryUrl() {
            this.country = new Text();
            this.count = new IntWritable();
        }

        public void write(DataOutput out) throws IOException {
            this.country.write(out);
            this.count.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            this.country.readFields(in);
            this.count.readFields(in);
        }

        public int compareTo(CountryUrl o) {
            int cmp = country.compareTo(o.country);
            if (cmp != 0) {
                return cmp;
            } else {
                return -count.compareTo(o.count);
            }
        }

        @Override
        public String toString() {
            return String.join(",", country.toString(), count.toString());
        }

        @Override
        public int hashCode() {
            return country.hashCode() * 31 + count.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CountryUrl) {
                CountryUrl o = (CountryUrl) obj;
                return country.equals(o.country) && count.equals(o.count);
            }
            return false;
        }

    }

}


