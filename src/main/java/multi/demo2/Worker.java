package multi.demo2;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by Leo on 2017-2-23.
 */
public class Worker implements  Runnable{

    private ConsumerRecord<String,String> consumerRecord;

    public Worker(ConsumerRecord<String,String> record){
        this.consumerRecord = record;
    }


    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " consumed " + consumerRecord.partition()+
                "th message with offset: " + consumerRecord.offset()+"  value:"+consumerRecord.value());
    }
}
