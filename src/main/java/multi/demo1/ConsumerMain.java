package multi.demo1;

/**
 * Created by Leo on 2017-2-23.
 */
public class ConsumerMain {


    public static void main(String[] args) {
        String brokerList = "172.20.4.233:9092";
        String groupId = "127.0.0.1.daijie";
        String topic = "topic201702331414";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum,groupId,topic,brokerList);
        consumerGroup.execute();
    }

}
