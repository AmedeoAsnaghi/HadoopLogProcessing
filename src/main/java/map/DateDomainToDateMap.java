package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import utils.DateDomainWritable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Created by ame on 19/02/15.
 *  Number of referring domains per day (between the 22nd of April and the 30th of May)
 */
public class DateDomainToDateMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {


    @Override
    public void map(LongWritable key, Text text, OutputCollector<LongWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(text.toString(),",");
        if(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            Long date = Long.parseLong(token);
            outputCollector.collect(new LongWritable(date), new IntWritable(1));
        }
    }
}
