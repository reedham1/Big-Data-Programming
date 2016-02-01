package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.util.*;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import com.refactorlabs.cs378.sessions.*;

/**
 * Created by Jong Hoon Lim on 10/26/15.
 */
public class MultipleInputsJoin extends Configured implements Tool {

    /*
    Mapper to read in the AVRO file produced by the modified SessionWriter Class
     */
    public static class SessionMapperClass
            extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();

            // How many unique users are there?
            HashMap<String, Long> uniqueUsers = new HashMap<>();

            // How many unique user clicks are there? And what are their subtypes?
            HashMap<String, HashMap<CharSequence, Long>> userClicks = new HashMap<>();

            // for show_badge_detail
            HashSet<String> showBadge = new HashSet<>();
            // for edit_contact_form
            HashSet<String> editContact = new HashSet<>();
            // for submit_contact_form
            HashSet<String> submitContact = new HashSet<>();

            // look through events of the Session Avro output file
            for (Event event : session.getEvents()) {
                String vin = event.getVin().toString();
                // store unique users since HashMaps don't allow duplicate keys
                uniqueUsers.put(vin, 1L);

                // CHECK event case
                if (event.getEventType() == EventType.CLICK) {
                    // if this vin already exists in the map
                    if (userClicks.containsKey(vin)) {
                        HashMap<CharSequence, Long> thisClick = userClicks.get(vin);
                        thisClick.put(event.getEventSubtype().name().toString(), 1L);
                    }
                    // this vin doesn't exist in the map yet
                    else {
                        HashMap<CharSequence, Long> thisClick = new HashMap<>();
                        thisClick.put(event.getEventSubtype().name().toString(), 1L);
                        userClicks.put(vin, thisClick);
                    }
                }
                // for SHOW event
                else if (event.getEventType() == EventType.SHOW && event.getEventSubtype() == EventSubtype.BADGE_DETAIL)
                    showBadge.add(vin);
                    // for EDIT event
                else if (event.getEventType() == EventType.EDIT)
                    editContact.add(vin);
                    // for SUBMIT event
                else if (event.getEventType() == EventType.SUBMIT)
                    submitContact.add(vin);
            }

            for (String vin : uniqueUsers.keySet()) {
                VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();
                vinImpressionBuilder.setUniqueUser(1L);
                // check to see if VIN exists in CLICK HashMap
                if (userClicks.containsKey(vin))
                    vinImpressionBuilder.setClicks(userClicks.get(vin));
                // check for the SHOW
                if (showBadge.contains(vin))
                    vinImpressionBuilder.setShowBadgeDetail(1L);
                // check for the EDIT event type
                if (editContact.contains(vin))
                    vinImpressionBuilder.setEditContactForm(1L);
                // check for the SUBMIT event type
                if (submitContact.contains(vin))
                    vinImpressionBuilder.setSubmitContactForm(1L);

                // pass on to reducer
                word.set(vin);
                context.write(word, new AvroValue(vinImpressionBuilder.build()));
            }
        }
    }

    /*
    Mapper to read in the VIN dataSet7VinCounts.csv file
    Unique users either viewed search results page (SRP) or a vehicle detail page (VDP)
     */
    public static class VINMapperClass extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String split[] = line.split(",");

            // the very first line should not be read
            if (split[0].equals("vin"))
                return;

            // the current VIN
            String vin = split[0];
            word.set(vin);
            // either SRP or VDP
            String eventType = split[1];
            // the count for the current VIN
            Long count = Long.parseLong(split[2]);

            VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();
            // SRP
            if (split[1].equals("SRP"))
                vinImpressionBuilder.setMarketplaceSrps(count);
            else // VDP
                vinImpressionBuilder.setMarketplaceVdps(count);

            // pass on to reducer
            context.write(word, new AvroValue<>(vinImpressionBuilder.build()));
        }
    }

    /*
    shares the Reduce Class for combining VinImpressionsCounts instances
     */
    public static class CombinerClass
            extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        public static VinImpressionCounts.Builder LeftOuterJoinCombiner(Text key, Iterable<AvroValue<VinImpressionCounts>> values)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();

            // since CLICK needs to be held in a map
            HashMap<CharSequence, Long> clickMap = new HashMap<>();

            for (AvroValue<VinImpressionCounts> value : values) {
                // for each instance of VinImpressionCounts
                VinImpressionCounts current = value.datum();

                // get other builder's click map
                Map<CharSequence, Long> otherClickMap = current.getClicks();
                if (otherClickMap != null) { // other click map exists, add elements

                    for (Map.Entry<CharSequence, Long> clickEntry : otherClickMap.entrySet()) {

                        CharSequence clickKey = clickEntry.getKey();
                        if (clickMap.containsKey(clickKey))
                            clickMap.put(clickKey, clickMap.get(clickKey) + otherClickMap.get(clickKey));
                        else
                            clickMap.put(clickKey, otherClickMap.get(clickKey));

                    }

                }

                // now take care of unique users, show_badge_details, edit_contact_form, submit_contact_form, srps, vdps
                vinImpressionBuilder.setUniqueUser(vinImpressionBuilder.getUniqueUser() + current.getUniqueUser());
                vinImpressionBuilder.setShowBadgeDetail(vinImpressionBuilder.getShowBadgeDetail()
                        + current.getShowBadgeDetail());
                vinImpressionBuilder.setEditContactForm(vinImpressionBuilder.getEditContactForm()
                        + current.getEditContactForm());
                vinImpressionBuilder.setSubmitContactForm(vinImpressionBuilder.getSubmitContactForm()
                        + current.getSubmitContactForm());
                vinImpressionBuilder.setMarketplaceSrps(vinImpressionBuilder.getMarketplaceSrps()
                        + current.getMarketplaceSrps());
                vinImpressionBuilder.setMarketplaceVdps(vinImpressionBuilder.getMarketplaceVdps()
                        + current.getMarketplaceVdps());
            }

            vinImpressionBuilder.setClicks(clickMap);

            return vinImpressionBuilder;
        }

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder vinImpressionBuilder = LeftOuterJoinCombiner(key, values);

            context.write(key, new AvroValue(vinImpressionBuilder.build()));

        }
    }

    /*
    perform the left outer join
     */
    public static class ReduceClass
            extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder vinImpressionBuilder = CombinerClass.LeftOuterJoinCombiner(key, values);

            // only write to output if there is at least 1 unique user
            if (vinImpressionBuilder.getUniqueUser() != 0)
                context.write(key, new AvroValue(vinImpressionBuilder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MultipleInputsJoin <input path> <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "MultipleInputsJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(MultipleInputsJoin.class);
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the multiple inputs for mappers.
        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VINMapperClass.class);

        // Specify input key schema for avro input type.
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());

        // Specify the Map
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Set combiner class.
        job.setCombinerClass(CombinerClass.class);

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        // FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new MultipleInputsJoin(), args);
        System.exit(res);
    }

}
