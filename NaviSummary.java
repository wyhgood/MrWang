


import com.hadoop.mapreduce.LzoTextInputFormat;
// import com.sogou.stat.common.util.DateUtils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import utils.DateUtils;
//import utils.SearchLogExtract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;
import org.mortbay.log.Log;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import com.go2map.mapservice.whereis.PointLocation;
import utils.SearchLogExtract;

// input: /user/go2maplog/logs/client_log/2015_02_09
// output: dje/output/sq-output
// local
// input:
// output:
public class NaviSummary extends Configured implements Tool {

    // enum TupleFields {
    // DATE, FROMSQ, EVENT, UVID
    // }
    /*
    enum TupleFields {
        FROMSQ, EVENT, UVID
    }
    */
    enum TupleFields {
         Date, NaviId, TotalDis, Time, RoadCondition
    }


    static public class SQActivityStatMapper
            extends Mapper<Object, Text, Tuple, NullWritable> {
        /*
        public static Text transformTextToUTF8(Text text, String encoding)
                throws UnsupportedEncodingException {
            try {
                text.set(new String(text.getBytes(), 0, text.getLength(), encoding));
                return text;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw e;
            }
        }
        */

        private Tuple _keyOut = new Tuple();
        //final static Pattern TIME_PATTERN =
        //Pattern.compile("time=(\\d{13})");
        //String tag = "weixingo2map,sousuo,daohang,shoushu,qqshuru,haomatong,wenwen,shoujizhushou,bizhi,baike,xingchen,haha,ceshi,lianmeng,pushios,pushandroid,pushxiaomi,ClientSq,happy";

        //HashSet<String> _FromSet = new HashSet<String>(Arrays.asList("weixingo2map", "sousuo", "daohang", "shoushu", "qqshuru", "haomatong", "wenwen", "shoujizhushou", "bizhi", "baike", "xingchen", "haha", "lianmeng", "pushios", "pushxiaomi","ClientSq", "happy"));

        //String info = "{\"b\":0,\"l\":[{\"t\":1412992080099,\"i\":28,\"action\":\"0\",\"e\":\"301\",\"loc\":\"11871601.414562,3051638.501395\",\"for\":\"-1\"},{\"i\":29,\"e\":\"1217\",\"loc\":\"11871601.414562,3051638.501395\",\"t\":1412992080100},{\"i\":30,\"e\":\"58\",\"loc\":\"11871601.414562,3051638.501395\",\"t\":1412992080205},{\"i\":31,\"e\":\"1201\",\"loc\":\"11871601.414562,3051638.501395\",\"t\":1412992080228},{\"i\":32,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11871570.399005,3051579.737557\",\"t\":1412992085207},{\"i\":33,\"e\":\"1413\",\"loc\":\"11871570.399005,3051579.737557\",\"t\":1412992087846},{\"i\":34,\"e\":\"1212\",\"loc\":\"11871570.399005,3051579.737557\",\"t\":1412992087857},{\"ort\":\"2\",\"i\":35,\"e\":\"1401\",\"loc\":\"11871570.399005,3051579.737557\",\"t\":1412992088015},{\"i\":36,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11871562.197238,3051579.358804\",\"t\":1412992092930},{\"i\":37,\"action\":\"3\",\"e\":\"301\",\"loc\":\"11871557.989050,3051577.657012\",\"t\":1412992095866},{\"i\":38,\"action\":\"10\",\"by\":\"2\",\"e\":\"301\",\"loc\":\"11871557.989050,3051577.657012\",\"t\":1412992097231},{\"ort\":\"2\",\"i\":39,\"e\":\"1405\",\"loc\":\"11871543.526320,3051568.525448\",\"t\":1412992102653},{\"i\":40,\"dur\":\"95\",\"e\":\"2800\",\"loc\":\"11871487.802821,3051536.803688\",\"t\":1412992121136},{\"i\":41,\"e\":\"59\",\"loc\":\"11871487.802821,3051536.803688\",\"t\":1412992121137},{\"ort\":\"2\",\"i\":42,\"e\":\"1414\",\"loc\":\"11871487.802821,3051536.803688\",\"t\":1412992121268},{\"i\":43,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11871487.802821,3051536.803688\",\"t\":1412992123724},{\"ort\":\"2\",\"i\":44,\"e\":\"1415\",\"loc\":\"11871354.876338,3051506.866796\",\"t\":1412992183342},{\"i\":45,\"e\":\"56\",\"loc\":\"11871354.876338,3051506.866796\",\"t\":1412992187375},{\"i\":46,\"dur\":\"2\",\"e\":\"2800\",\"loc\":\"11871355.305554,3051505.030116\",\"t\":1412992190318},{\"i\":47,\"e\":\"59\",\"loc\":\"11871355.305554,3051505.030116\",\"t\":1412992190318},{\"ort\":\"2\",\"i\":48,\"e\":\"1414\",\"loc\":\"11871355.305554,3051505.030116\",\"t\":1412992190503},{\"i\":49,\"dur\":\"0\",\"e\":\"2800\",\"loc\":\"11871355.305554,3051505.030116\",\"t\":1412992191509},{\"i\":50,\"e\":\"59\",\"loc\":\"11871355.305554,3051505.030116\",\"t\":1412992191509},{\"i\":2,\"t\":1412994709343,\"e\":\"56\"},{\"i\":3,\"t\":1412994709382,\"e\":\"16\"},{\"i\":4,\"t\":1412994709603,\"e\":\"10\"},{\"mcc\":\"460\",\"t\":1412994709676,\"mnc\":\"01\",\"vAccuracy\":\"61.217803\",\"hAccuracy\":\"165.000000\",\"timescape\":\"0.344470\",\"i\":5,\"heading\":\"-1.000000\",\"location\":\"106.729116,26.517130\",\"speed\":\"-1.000000\"},{\"i\":6,\"e\":\"11\",\"loc\":\"11881156.946164,3044009.659677\",\"t\":1412994710634},{\"from\":\"4\",\"i\":7,\"action\":\"0\",\"e\":\"801\",\"loc\":\"11881156.946164,3044009.659677\",\"t\":1412994710998},{\"i\":8,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11881157.611996,3044004.806377\",\"t\":1412994715246},{\"t\":1412994715512,\"from\":\"4\",\"by\":\"0\",\"i\":9,\"action\":\"2\",\"e\":\"801\",\"keyword\":\"?????\",\"loc\":\"11881157.611996,3044004.806377\"},{\"t\":1412994716709,\"i\":10,\"action\":\"0\",\"e\":\"301\",\"loc\":\"11881156.604271,3044005.501247\",\"for\":\"-1\"},{\"i\":11,\"e\":\"58\",\"loc\":\"11881156.604271,3044005.501247\",\"t\":1412994716864},{\"i\":12,\"e\":\"1201\",\"loc\":\"11881156.604271,3044005.501247\",\"t\":1412994716890},{\"i\":13,\"e\":\"1413\",\"loc\":\"11881159.683433,3044002.747697\",\"t\":1412994720480},{\"i\":14,\"e\":\"1212\",\"loc\":\"11881159.683433,3044002.747697\",\"t\":1412994720487},{\"ort\":\"2\",\"i\":15,\"e\":\"1401\",\"loc\":\"11881159.683433,3044002.747697\",\"t\":1412994720644},{\"i\":16,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11881147.842656,3044012.978874\",\"t\":1412994725566},{\"i\":17,\"action\":\"3\",\"e\":\"301\",\"loc\":\"11881141.273777,3044010.334220\",\"t\":1412994762156},{\"i\":18,\"action\":\"9\",\"by\":\"2\",\"e\":\"301\",\"loc\":\"11881141.273777,3044010.334220\",\"t\":1412994769673},{\"i\":19,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11881141.796301,3044011.459494\",\"t\":1412994785142},{\"i\":20,\"action\":\"15\",\"e\":\"301\",\"loc\":\"11881141.796301,3044011.459494\",\"t\":1412994793210},{\"i\":21,\"dur\":\"195\",\"e\":\"2800\",\"loc\":\"11881175.293850,3043906.404917\",\"t\":1412994904284},{\"i\":22,\"e\":\"59\",\"loc\":\"11881175.293850,3043906.404917\",\"t\":1412994904285},{\"ort\":\"2\",\"i\":23,\"e\":\"1414\",\"loc\":\"11881175.293850,3043906.404917\",\"t\":1412994904344},{\"i\":24,\"e\":\"56\",\"loc\":\"11881192.555818,3043825.370463\",\"t\":1412994916499},{\"ort\":\"2\",\"i\":25,\"e\":\"1405\",\"loc\":\"11881767.351356,3041362.165648\",\"t\":1412995188370},{\"i\":26,\"dur\":\"290\",\"e\":\"2800\",\"loc\":\"11881661.932052,3041371.352795\",\"t\":1412995206922},{\"i\":27,\"e\":\"59\",\"loc\":\"11881661.932052,3041371.352795\",\"t\":1412995206922},{\"ort\":\"2\",\"i\":28,\"e\":\"1414\",\"loc\":\"11881661.932052,3041371.352795\",\"t\":1412995206961}],\"u\":\"1411393267306504\",\"r\":450,\"kd\":\"c292aa689ead280bda22b50f62e70721fda9c0bc\"}";

        //String info2 = "{\"b\":0,\"l\":[{\"i\":6,\"e\":\"2801\",\"loc\":\"13403335.614952,3684620.080711\",\"t\":1412216156089}],\"u\":\"14122161524281074\",\"kd\":\"1cbb9392f8bc9984176ac21e8d6f6b6a44940259\"}";


        @Override
        public final void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String valStr = value.toString();
            //System.out.println(valStr);
            //String valStr = transformTextToUTF8(value, "GBK").toString();
            int totalDis = 0;
            int tiqianTime = 0;
            int totalTime = 0;
            String naviId = null;
            String date = null;
            String time_consume_str = null;
            String totaldis_str = null;
            String roadCondition_str = null;
            //System.out.println(valStr.substring(valStr.length()-1,valStr.length()).equals("}"));
            if (true) {
                    try {
                        int totaldis = 0;
                        if (valStr.indexOf("navSummary") > 0 && valStr.indexOf("moblog") > 0) {
                            //System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");


                            valStr = valStr.substring(valStr.indexOf("datas=") + 7, valStr.indexOf("moblog") - 2);
                            ObjectMapper mapper = new ObjectMapper();
                            //System.out.println(valStr);
                            JsonNode rootNode = mapper.readTree(valStr);
                            naviId = rootNode.path("ucNavigateId").toString();

                            int time_consume = 0;
                            int roadCondition = 0;
                            //System.out.println(valStr);
                            //System.out.println("rootNode\t" + rootNode.path("data").path("tiqianTime").asInt(
                            //));
                           // System.out.println(rootNode.path("data").path("tiqianTime").toString() != "");
                            //System.out.println(rootNode.path("data").path("totaltime").toString() != "");
                            if (rootNode.path("data").path("tiqianTime").toString() != "" && rootNode.path("data").path("totaltime").toString() != "") {

                                tiqianTime = Integer.parseInt(rootNode.path("data").path("tiqianTime").toString());
                                totalTime = Integer.parseInt(rootNode.path("data").path("totaltime").toString());
                                //System.out.println("tiqianTime\t" + tiqianTime);
                                //System.out.println("totaltime\t" + totalTime);
                                if (totalTime > 0) {
                                    if (tiqianTime == -1) {
                                        tiqianTime = 0;
                                    }


                                    time_consume = totalTime * 100 / (totalTime + tiqianTime);
                                    //System.out.println("time_consume" + time_consume);
                                    time_consume_str = Integer.toString(time_consume);
                                }
                            }

                            if (rootNode.path("data").path("totalDis").toString() != "") {
                                //System.out.println("total\t" + rootNode.path("data").path("totalDis").asInt());
                                totaldis = Integer.parseInt(rootNode.path("data").path("totalDis").toString());
                                totaldis = totaldis / 1000;

                                totaldis_str = Integer.toString(totaldis);
                            }

                            if (rootNode.path("data").path("totalDis").toString() != "" && rootNode.path("data").path("jam").toString() != "") {
                                int tem = Integer.parseInt(rootNode.path("data").path("totalDis").toString());
                                if (tem > 0) {
                                    //System.out.println("jam\t" + rootNode.path("data").path("jam").asInt());
                                    roadCondition = Integer.parseInt(rootNode.path("data").path("jam").toString().trim().replace("\"","")) * 100 / tem;
                                    //System.out.println(roadCondition);
                                    roadCondition_str = Integer.toString(roadCondition);
                                }
                            }

                            //System.out.println("navId\t" + naviId);
                            if (naviId.indexOf("_") > 0) {
                                //System.out.println("navId\t" + naviId);
                                String startTime = naviId.split("_")[1];
                                if (startTime.length() > 13) {
                                    //System.out.println(startTime);
                                    startTime = startTime.substring(0, 13);
                                    long time = Long.parseLong(startTime);
                                    date = DateUtils.longToDateStr(time, "yyyy-MM-dd");
                                    //System.out.println(date);
                                }
                            }


                            //_keyOut.setString(TupleFields.Complete, complete);}


                            _keyOut.setString(TupleFields.Time, "%" + time_consume_str);
                            _keyOut.setString(TupleFields.RoadCondition, "%" + roadCondition_str);
                            _keyOut.setString(TupleFields.TotalDis, totaldis_str + "-kilo");
                            _keyOut.setString(TupleFields.Date, date);
                            _keyOut.setString(TupleFields.NaviId, naviId);
                            if (totaldis >= 1) {
                                context.write(_keyOut, NullWritable.get());
                            }
                        }
                    }catch (Exception e){
                        System.out.println(valStr);
                        System.out.println(e);
                    }

            }

        }

        }






static public class SQActivityStatReducer
        extends Reducer<Tuple, NullWritable, Text, NullWritable> {
    private Text _keyOut = new Text();

    @Override
    public final void reduce(Tuple key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        String road = key.getString(TupleFields.RoadCondition);
        String time = key.getString(TupleFields.Time);
        String totaldis = key.getString(TupleFields.TotalDis);
        //String startPoint = key.getString(TupleFields.StartPo);
        //String endPoint = key.getString(TupleFields.EndPoint);
        //String isAcross = key.getString(TupleFields.);
        String date = key.getString(TupleFields.Date);
        //String event = key.getString(TupleFields.EVENT);
        int pvCnt = 0;

        //String preUvid = null;
        //String curUvid = null;
        //Log.info("_________________________" + "\t");
        for (NullWritable value : values){
            pvCnt++;
        }
        _keyOut.set(date + "\t" +road + "\t" + time + "\t" + totaldis  +"\t" + pvCnt);
        context.write(_keyOut, NullWritable.get());
    }
}

    public static void setupSecondarySort(Configuration conf) {
        ShuffleUtils.configBuilder()
                .useNewApi()
                .setPartitionerIndices(TupleFields.Date, TupleFields.RoadCondition, TupleFields.Time, TupleFields.TotalDis)
                .setSortIndices(TupleFields.values())
                .setGroupIndices(TupleFields.Date, TupleFields.RoadCondition, TupleFields.Time, TupleFields.TotalDis)
                .configure(conf);
    }

    public int run(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            System.out.println("args[" + i + "]" + args[i]);
        }

        if (args.length < 2) {
            System.err.println("Usage: hadoop jar path/to/this.jar " + getClass() + " <input dir> <output dir>");
            return -1;
        }

        Configuration conf = new Configuration();
        if (args[args.length - 1].equals("p")) {
            conf.set("io.compression.codecs",
                    "com.hadoop.compression.lzo.LzopCodec");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        }
        setupSecondarySort(conf);

        Job job = new Job(conf, "TiaoXiG stat");


        job.setJarByClass(NaviSummary.class);

        for (int i = 0; i < args.length - 2; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        if (args[args.length - 1].equals("p")) {
            job.setInputFormatClass(LzoTextInputFormat.class);
        } else {
            job.setInputFormatClass(TextInputFormat.class);
        }

        job.setMapperClass(SQActivityStatMapper.class);
        job.setReducerClass(SQActivityStatReducer.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);

        Path output = new Path(args[args.length - 2]);
        FileOutputFormat.setOutputPath(job, output);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NaviSummary(), args);
        System.exit(res);
    }

}
