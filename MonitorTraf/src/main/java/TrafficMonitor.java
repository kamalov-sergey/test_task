import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.hive.ql.parse.HiveParser.colType_return;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import scala.Tuple2;

public class TrafficMonitor { 
	public static boolean flagPrevAlert=false;
	
    public static void main(String[] args)  {
    	
    	SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("MonitorTraf").set("spark.driver.allowMultipleContexts","true");
    	SparkContext sc = new SparkContext(conf);
    	JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(5));
    	HiveContext hiveContext = new HiveContext(sc);
    	Dataset<Row> tableLimit = hiveContext.table("traffic_limits.limits_per_hour");
        final Integer min = tableLimit.filter(tableLimit.col("limit_name").equalTo("min")).select(tableLimit.col("limit_value")).first().getInt(0);
        final Integer max = tableLimit.filter(tableLimit.col("limit_name").equalTo("max")).select(tableLimit.col("limit_value")).first().getInt(0);
        //sudo spark2-submit --class TrafficMonitor --master yarn --deploy-mode client --conf spark.driver.allowMultipleContexts=true MonitorTraf-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
        //System.out.println(min+"CHECKCHECK"+max);
        
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("traffic");

        JavaInputDStream<ConsumerRecord<Integer, Integer>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));
        
        JavaDStream<Integer> sumBytesStream = messages.map(ConsumerRecord::value).map(s -> s+s);
        Integer sumBytes = Integer.parseInt(sumBytesStream.toString());
        if(sumBytes > max || sumBytes < min){SendToKafka.sendToKafka("alert","AAALAAAARM traffic is:"+sumBytes);flagPrevAlert = true;}
                else if(flagPrevAlert = true)
                {
                	SendToKafka.sendToKafka("alert","Don't worry!");
                	flagPrevAlert = false;
                };
          
        jssc.start();
        jssc.awaitTermination();
    }
}

