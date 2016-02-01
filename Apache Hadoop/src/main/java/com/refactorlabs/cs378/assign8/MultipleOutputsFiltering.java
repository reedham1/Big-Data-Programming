package com.refactorlabs.cs378.assign8;

import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.utils.Utils;

import com.refactorlabs.cs378.sessions.*;

/**
 * Created by Jong Hoon Lim on 10/27/15.
 */

public class MultipleOutputsFiltering extends Configured implements Tool {

    // for the counter
    public static final String WRITE_COUNTER = "write counter";

    // get the session type name from the SessionType enum class
    public static final String SUBMITTER = SessionType.SUBMITTER.getText();
    public static final String CLICKER = SessionType.CLICKER.getText();
    public static final String CPO = SessionType.CPO.getText();
    public static final String SHOWER = SessionType.SHOWER.getText();
    public static final String VISITOR = SessionType.VISITOR.getText();
    public static final String OTHER = SessionType.OTHER.getText();


    public static class MapClass
            extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        // for random sampling
        private Random random = new Random();

        private AvroMultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new AvroMultipleOutputs(context);
        }

        public void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws InterruptedException, IOException{
            multipleOutputs.close();
        }

        private Text word = new Text();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();

            // boolean to keep up with what kind of event type
            boolean isSubmitter = false;
            boolean isCPO = false;
            boolean isClicker = false;
            boolean isShower = false;
            boolean isVisitor = false;

            // iterate through the events and find corresponding category
            for (Event event : session.getEvents()) {

                // output not correct with if else statements
                // use switch
                switch(event.getEventType()) {
                    case CHANGE:
                    case CONTACT_FORM_STATUS:
                    case EDIT:
                    case SUBMIT:
                        isSubmitter = true;
                        break;
                    case ILMR_CPO:
                        isCPO = true;
                        break;
                    case CLICK:
                    case PLAY:
                    case PRINT:
                        isClicker = true;
                        break;
                    case SHOW:
                        isShower = true;
                        break;
                    case VISIT:
                        isVisitor = true;
                        break;
                    default:
                        break;
                }

            }

            // submitter case
            if (isSubmitter) {
                multipleOutputs.write(SUBMITTER, key, value);
                context.getCounter(WRITE_COUNTER, SUBMITTER).increment(1L);
            }
            // cpo case
            else if (isCPO) {
                multipleOutputs.write(CPO, key, value);
                context.getCounter(WRITE_COUNTER, CPO).increment(1L);
            }
            // clicker case
            else if (isClicker) {
                multipleOutputs.write(CLICKER, key, value);
                context.getCounter(WRITE_COUNTER, CLICKER).increment(1L);
            }
            // shower case
            else if(isShower) {
                multipleOutputs.write(SHOWER, key, value);
                context.getCounter(WRITE_COUNTER, SHOWER).increment(1L);
            }
            // visitor case
            else if (isVisitor) {
                multipleOutputs.write(VISITOR, key, value);
                context.getCounter(WRITE_COUNTER, VISITOR).increment(1L);
            }
            // all other session event types (OTHER) = sample 1 in 1000... discard others
            else {
                // random value from 1 through 1000
                int randomValue = random.nextInt(1000) + 1;
                // 7 will occur with probability of 1 in 1000
                if (randomValue == 7) {
                    multipleOutputs.write(OTHER, key, value);
                    context.getCounter(WRITE_COUNTER, OTHER).increment(1L);
                }
                // if not randomly sampled, count the amount discared by filter
                else
                    context.getCounter("discard counter", "discarded OTHER sessions").increment(1L);
            }

        }

    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MultipleOutputsFiltering <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "MultipleOutputsFiltering");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(MultipleOutputsFiltering.class);
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        job.setMapperClass(MapClass.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify input key schema for avro input type.
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());

        // specify output key schema for avro input type
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        AvroMultipleOutputs.setCountersEnabled(job, true);

        MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, Text.class);

        // output sessions to different files based on category
        // submitter
        AvroMultipleOutputs.addNamedOutput(job, SUBMITTER, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        // clicker
        AvroMultipleOutputs.addNamedOutput(job, CLICKER, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        // cpo
        AvroMultipleOutputs.addNamedOutput(job, CPO, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        // shower
        AvroMultipleOutputs.addNamedOutput(job, SHOWER, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        // visitor
        AvroMultipleOutputs.addNamedOutput(job, VISITOR, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        // other
        AvroMultipleOutputs.addNamedOutput(job, OTHER, AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        String[] inputPaths = appArgs[0].split(",");
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new MultipleOutputsFiltering(), args);
        System.exit(res);
    }
}
