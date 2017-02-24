package multi.demo2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Leo on 2017-2-23.
 */
public class ConsumerHandler {

    private final KafkaConsumer<String,String> consumer;
    private ExecutorService executorService;


    public ConsumerHandler(String brokerList,String groupId,String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");        //本例使用自动提交位移
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<String,String> (props);
        consumer.subscribe(Arrays.asList(topic));   // 本例使用分区副本自动分配策略
    }


    public  void  execute(int workerNum){
        executorService = new ThreadPoolExecutor(workerNum,workerNum,0L, TimeUnit.MILLISECONDS,new ArrayBlockingQueue<Runnable>(1000),new ThreadPoolExecutor.CallerRunsPolicy());

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(200);
            for(final  ConsumerRecord record :records){
                executorService.submit(new Worker(record));
            }
        }
    }


    public  void shutdown(){
        if(consumer != null){
            consumer.close();
        }
        if(executorService != null){
            executorService.shutdown();
        }
        try{
            if(!executorService.awaitTermination(10,TimeUnit.SECONDS)){
                System.out.println("Timeout ... Ignore for this case");
            }
        }catch (InterruptedException e){
            System.out.println("Other thread interrupted this shutdown, ignore for this case.");
            Thread.currentThread().interrupt();
        }
    }


}
