package job; /**
 * Created by ame on 13/02/15.
 During April of 2003 one of Internet's first viral videos was born.
 It was the video of a kid pretending to wield a Star Wars lightsaber.
 For many years the video remained the top viral video on the Internet. The video's name was "Star Wars Kid".

 Andy Baio, creater of XOXO and former CTO of Kickstarter, played an important role in spreading the viral video.
 He has made the logs of his Waxy.org Apache Servers available so that we can study the spreading phenomenon.
 You can download the logs at the following URL: http://home.deib.polimi.it/guinea/middleware/star_wars_data.zip.
 The file is more or less 160 MB in zipped form, and 1.6 GB in unziped form.
 The data goes from the 10th of April to the 26th of November; the video was posted to Waxy.org on the 29th of April.
 Among other things the log provides dates, times, IP addresses, user agents, and referer information.

 Using Hadoop in a fully distributed cluster (use your own èphisical/virtual machines) provide the following information:

 Waxy.org total pageviews per day in the entire time range
 Video downloads (wmv files) per day in the entire time range. (Aggregate the two video versions - normal and remixed)
 Number of referring domains per day (between the 22nd of April and the 30th of May)
 Number of referrals per domain (between the 22nd of April and the 30th of May)
 Optional: Provide charts for the information

 */

import map.DateDomainToDateMap;
import map.ReferringCountMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reduce.ReferringCountReduce;
import reduce.SumPerDateReduce;
import utils.DateDomainWritable;

public class ReferringCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new ReferringCount(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf1 = new JobConf(getConf(), ReferringCount.class);
        JobConf jobConf2 = new JobConf(getConf(), ReferringCount.class);

        //First job
        jobConf1.setJarByClass(getClass());
        Path in = new Path(args[0]);
        Path out = new Path("temp");

        FileInputFormat.setInputPaths(jobConf1, in);
        FileOutputFormat.setOutputPath(jobConf1, out);
        jobConf1.setJobName("ReferringCount - part 1");


        jobConf1.setMapperClass(ReferringCountMap.class);
        jobConf1.setReducerClass(ReferringCountReduce.class);

        jobConf1.setMapOutputKeyClass(DateDomainWritable.class);
        jobConf1.setMapOutputValueClass(IntWritable.class);

        jobConf1.setOutputKeyClass(DateDomainWritable.class);
        jobConf1.setOutputValueClass(IntWritable.class);

        Job job1 = new Job(jobConf1);

        //Second job
        jobConf2.setJarByClass(getClass());

        FileInputFormat.setInputPaths(jobConf2, new Path("temp/part-00000"));
        FileOutputFormat.setOutputPath(jobConf2, new Path("Out-ReferringCount"));
        jobConf2.setJobName("ReferringCount - part 2");

        jobConf2.setMapperClass(DateDomainToDateMap.class);
        jobConf2.setReducerClass(SumPerDateReduce.class);

        jobConf2.setOutputKeyClass(LongWritable.class);
        jobConf2.setOutputValueClass(IntWritable.class);

        Job job2 = new Job(jobConf2);

        JobControl jobControl = new JobControl("jobControl");
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        job2.addDependingJob(job1);

        jobControl.run();
        return 0;
    }
}
