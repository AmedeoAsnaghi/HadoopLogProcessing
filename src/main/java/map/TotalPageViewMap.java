package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Created by ame on 16/02/15.
 * TODO: MAPPER - Waxy.org total pageviews per day in the entire time range (-> Number of GETs)

 */
public class TotalPageViewMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {

    private static final String lower = "10/Apr/2003";
    private static final String upper = "26/Nov/2003";
    private final DateFormat dateFormat;

    public TotalPageViewMap(){
        super();
        dateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    }
    @Override
    public void map(LongWritable key, Text text, OutputCollector<LongWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(text.toString());
        StringTokenizer littleTokenizer;
        int i;
        boolean isGet = false;
        String line, date ="";
        for(i=0; tokenizer.hasMoreTokens(); i++){
            line = tokenizer.nextToken();
            //take the third element (date and time)
            if(i == 3){
                //take only the data without the time
                littleTokenizer = new StringTokenizer(line, ":");
                date = littleTokenizer.nextToken().replace("[","");

            }
            if(i == 5){
                if(line.contains("GET")){
                    isGet = true;
                }
            }
        }

        if(isGet)
        {
            try {
                Date lowerDate = dateFormat.parse(lower);
                Date upperDate = dateFormat.parse(upper);
                Date dateCheck = dateFormat.parse(date);

                if((dateCheck.before(upperDate) && dateCheck.after(lowerDate)) ||
                        dateCheck.equals(upperDate) || dateCheck.equals(lowerDate)){
                    outputCollector.collect(new LongWritable(dateCheck.getTime()), new IntWritable(1));
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
}
