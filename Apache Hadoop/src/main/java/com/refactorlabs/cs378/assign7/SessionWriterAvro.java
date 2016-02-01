package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.lang.StringUtils;
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
import com.refactorlabs.cs378.sessions.*;

/**
 * Created by Jong Hoon Lim on 10/13/15.
 */
public class SessionWriterAvro extends Configured implements Tool {

    // data set headers from dataSetHeaders.tsv
    public final static String[] dataSetHeaders = new String[]{"user_id", "event_type",	"page", "referrer",
            "referring_domain", "event_timestamp", "city", "region", "vin",	"vehicle_condition", "year", "make",
            "model", "trim",	"body_style", "subtrim", "cab_style", "initial_price", "mileage", "mpg",
            "exterior_color", "interior_color", "engine_displacement", "engine", "transmission", "drive_type", "fuel",
            "image_count", "initial_carfax_free_report", "carfax_one_owner","initial_cpo", "features"};

    /**
     * Map class for WordStatistics that uses AVRO generated class WordStatisticsData
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // get all tokens by splitting them by the tab characters
            String[] tokens = line.split("\\t");

            Session.Builder builder = Session.newBuilder();
            Event event = new Event();

            // first token of each line is user's ID... set User Id
            String userId = tokens[0];
            builder.setUserId(userId);

            // get the event and event subtype
            String[] events = tokens[1].split(" ");

            // use the helper method to set the event and subtype
            eventSetter(events, event, tokens[1]);

            Class<?> eventClass = event.getClass();

            // for the rest, start with second token
            for (int index = 2; index < tokens.length; index++) {
                // check for features
                String token = tokens[index];
                if (index == tokens.length - 1) {
                    String[] features = token.split(":");
                    List<CharSequence> allFeatures = new ArrayList<CharSequence>(Arrays.asList(features));
                    event.setFeatures(allFeatures);
                }
                // mpg case
                else if (dataSetHeaders[index].equals("mpg")) {
                    int mpg = Integer.parseInt(token);
                    event.setMpg(mpg);
                }
                // year case
                else if (dataSetHeaders[index].equals("year")) {
                    int year = Integer.parseInt(token);
                    event.setYear(year);
                }
                // image_count case
                else if (dataSetHeaders[index].equals("image_count")) {
                    int imageCount = Integer.parseInt(token);
                    event.setImageCount(imageCount);
                }
                // initial_price case
                else if (dataSetHeaders[index].equals("initial_price")) {
                    double price = Double.parseDouble(token);
                    //event.setInitialPrice(price);
                }
                // mileage case
                else if (dataSetHeaders[index].equals("mileage")) {
                    int mileage = Integer.parseInt(token);
                    event.setMileage(mileage);
                }
                else {
                    try {
                        // not a feature, regular data
                        // use the field class to set event
                        Field field = eventClass.getField(dataSetHeaders[index]);
                        field.set(event, token);
                    } catch (IllegalAccessException | NoSuchFieldException | IllegalArgumentException exception) {

                    };
                }
            }
            // make an ArrayList of events to pass on to reducer
            List<Event> eventArrayList = new ArrayList<Event>(Arrays.asList(event));
            builder.setEvents(eventArrayList);
            // the first token
            word.set(userId);
            context.write(word, new AvroValue(builder.build()));
        }

        // helper method for the mapper to set the event and the event subtype
        // for weird cases that contain '_' in the token
        private void eventSetter (String[] events, Event event, String fullEventString) {
            // events that contains "ilmr_cpo"
            if (events[0].contains("ilmr_cpo")) {
                String[] ilmrSplit = events[0].split("_");
                event.setEventType(EventType.valueOf("ILMR_CPO"));
                String subtype = ilmrSplit[2].toUpperCase();
                event.setEventSubtype(EventSubtype.valueOf(subtype));
            }
            // weird case with "contact form"
            else if (fullEventString.equals("contact form error") || fullEventString.equals("contact form success")) {
                event.setEventType(EventType.valueOf("CONTACT_FORM_STATUS"));
                String subtype = events[2].toUpperCase();
                event.setEventSubtype(EventSubtype.valueOf(subtype));
            }
            // weird case with "click contact"
            else if (fullEventString.equals("click contact banner") || fullEventString.equals("click contact button")) {
                event.setEventType(EventType.valueOf("CLICK"));
                String subtype = events[2].toUpperCase();
                event.setEventSubtype(EventSubtype.valueOf("CONTACT_BUTTON"));
            }
            // another weird case with "click contact"
            else if (fullEventString.equals("click vehicle history report link")) {
                event.setEventType(EventType.valueOf("CLICK"));
                event.setEventSubtype(EventSubtype.valueOf("VEHICLE_HISTORY"));
            }
            // click_on_alternative case
            else if (events[0].equals("click_on_alternative")) {
                event.setEventType(EventType.valueOf("CLICK"));
                event.setEventSubtype(EventSubtype.valueOf("ALTERNATIVE"));
            }
            // display_ilmr_report_listing case
            else if (events[0].equals("display_ilmr_report_listing") || events[0].equals("alternatives_displayed")) {
                event.setEventType(EventType.valueOf("DISPLAY"));
                if (events[0].contains("ilmr"))
                    event.setEventSubtype(EventSubtype.valueOf("ILMR_REPORT"));
                else
                    event.setEventSubtype(EventSubtype.valueOf("ALTERNATIVES"));
            }
            // error_loading_ilmr case
            else if (events[0].equals("error_loading_ilmr")) {
                event.setEventType(EventType.valueOf("ILMR_STATUS"));
                event.setEventSubtype(EventSubtype.valueOf("LOAD_ERROR"));
            }
            // ilmr_see_more_cpo case
            else if (events[0].equals("ilmr_see_more_cpo")) {
                event.setEventType(EventType.valueOf("ILMR_CPO"));
                event.setEventSubtype(EventSubtype.valueOf("SEE_MORE"));
            }
            // play_video_ilmr case
            else if (events[0].equals("play_video_ilmr")) {
                event.setEventType(EventType.valueOf("PLAY"));
                event.setEventSubtype(EventSubtype.valueOf("ILMR_VIDEO"));
            }
            // print_ilmr case
            else if (events[0].equals("print_ilmr")) {
                event.setEventType(EventType.valueOf("PRINT"));
                event.setEventSubtype(EventSubtype.valueOf("ILMR"));
            }
            // visit_market_report_listing case
            else if (events[0].equals("visit_market_report_listing")) {
                event.setEventType(EventType.valueOf("VISIT"));
                event.setEventSubtype(EventSubtype.valueOf("MARKET_REPORT"));
            }
            else { // everything else
                event.setEventType(EventType.valueOf(events[0].toUpperCase()));
                String eventSubtype = StringUtils.join(Arrays.copyOfRange(events, 1, events.length), "_");
                event.setEventSubtype(EventSubtype.valueOf(eventSubtype.toUpperCase()));
            }
        }

    }

    /**
     * The Reduce class for WordStatistics. Extends class Reducer, provided by Hadoop.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>,
            AvroValue<Session>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {

            Session.Builder builder = Session.newBuilder();
            // set user ID
            builder.setUserId(key.toString());
            // ArrayList of events from the Mapper Class
            ArrayList<Event> allEvents = new ArrayList<Event>();

            // iterate through the given Iterable
            for (AvroValue<Session> value : values) {
                Session session = value.datum();
                // add to our ArrayList
                allEvents.add(session.events.get(0));
            }

            builder.setEvents(allEvents);

            // output for the reducer class
            context.write(new AvroKey(key.toString()), new AvroValue(builder.build()));
        }

    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SessionWriter <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "SessionWriter");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(SessionWriterAvro.class);
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
        int res = ToolRunner.run(new Configuration(), new SessionWriterAvro(), args);
        System.exit(res);
    }
}
