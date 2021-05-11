import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Optional;

class MyMapWritable extends MapWritable {

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}

public class TempStat {
    public static class TempStatMapper extends Mapper<Object, Text, Text, MapWritable> {
        private static Logger logger = Logger.getLogger(TempStat.class);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
                String filePath = ((FileSplit) context.getInputSplit()).getPath().getParent().toString();
                String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                Text station = new Text(fields[0].replaceAll("\"", ""));
                double temp = Double.parseDouble(fields[13].replaceAll("\"", "").replace(',', '.'));
                temp = (temp - 32)/1.8;
                MapWritable map = new MapWritable();
                map.put(new Text("fileName"), new Text(fileName));
                map.put(new Text("filePath"), new Text(filePath));
                map.put(new Text("station"), new Text(station));
                map.put(new Text("temp"), new DoubleWritable(temp));
                context.write(new Text(filePath.substring(filePath.lastIndexOf("/") + 1) + "_" + station.toString()),
                        map);

            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());

            }
        }
    }

    public static class CombinerClass extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Double> arr = new ArrayList<Double>();
            Double sum = 0d;
            Double min = 0d, max = 0d;
            Double avg = 0d;
            HashSet<String> fileNames = new HashSet<>();
            HashSet<String> filePaths = new HashSet<>();
            for (MapWritable map : values) {
                fileNames.add(map.get(new Text("fileName")).toString());
                filePaths.add(map.get(new Text("filePath")).toString());
                DoubleWritable dWritable = (DoubleWritable) map.get(new Text("temp"));
                arr.add(dWritable.get());
            }
            Integer len = arr.size();
            min = max = arr.get(0);
            for (double v : arr) {
                sum += v;
                if (min > v)
                    min = v;
                if (max < v)
                    max = v;
            }
            avg = sum / len;
            MapWritable map = new MapWritable();

            map.put(new Text("station"), new Text(key.toString().substring(key.toString().lastIndexOf("_") + 1)));
            map.put(new Text("maxTemp"), new DoubleWritable(max));
            map.put(new Text("minTemp"), new DoubleWritable(min));
            map.put(new Text("avgTemp"), new DoubleWritable(avg));
            map.put(new Text("count"), new IntWritable(len));
            map.put(new Text("sum"), new DoubleWritable(sum));


            context.write(new Text(key.toString().substring(0, key.toString().lastIndexOf("_"))), map);

        }
    }

    public static class TempStatReducer extends Reducer<Text, MapWritable, Text, Text> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            JSONObject outputJSONObject = new JSONObject();
            List<JSONObject> list = new ArrayList<>();
            try {
                
                for (MapWritable map : values) {
                    JSONObject json = new JSONObject();
                    json.put("station", map.get(new Text("station")).toString());
                    json.put("maxTemp", ((DoubleWritable) map.get(new Text("maxTemp"))).get());
                    json.put("minTemp", ((DoubleWritable) map.get(new Text("minTemp"))).get());
                    json.put("avgTemp", ((DoubleWritable) map.get(new Text("avgTemp"))).get());
                    json.put("count", ((IntWritable) map.get(new Text("count"))).get());
                    json.put("sum", ((DoubleWritable) map.get(new Text("sum"))).get());
                    list.add(json);
                }
                outputJSONObject.put("year", key.toString());
                outputJSONObject.put("stationsData",list);

            } catch (Exception e) {
                e.printStackTrace();
            }

            context.write(new Text(""), new Text(outputJSONObject.toString()));

        }
    }

    public static void main(String[] args)
            throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Temp stats");
        job.setJarByClass(TempStat.class);
        job.setMapperClass(TempStatMapper.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(TempStatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setNumReduceTasks(2);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setMapOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}