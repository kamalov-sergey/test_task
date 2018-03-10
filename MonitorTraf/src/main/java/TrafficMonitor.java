import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class TrafficMonitor { 
    public static boolean flagPrevAlert=false;
    
    public static void main(String[] args) throws InterruptedException  {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("MonitorTraf");
        SparkSession sc = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate();
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> tableLimit = sqlContext.table("traffic_limits.limits_per_hour");
        final Integer min = tableLimit.filter(tableLimit.col("limit_name").equalTo("min")).select(tableLimit.col("limit_value")).first().getInt(0);
        final Integer max = tableLimit.filter(tableLimit.col("limit_name").equalTo("max")).select(tableLimit.col("limit_value")).first().getInt(0);
        //sudo spark2-submit --class TrafficMonitor --master yarn --deploy-mode client --conf spark.driver.allowMultipleContexts=true MonitorTraf-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
        //System.out.println(min+"CHECKCHECK"+max);
        
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test_job");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("traffic");

        JavaInputDStream<ConsumerRecord<Integer, Integer>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));
				
        Function2<Integer, Integer, Integer> reduceSumFunc = (accum, n) -> (accum + n);
		
        JavaDStream<Integer> sumBytesStream = messages.map(ConsumerRecord::value).reduce(reduceSumFunc);
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