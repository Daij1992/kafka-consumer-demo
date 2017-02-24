package multi.demo2;

/**
 * Created by Leo on 2017-2-23.
 */
public class ConsumerMain {

    public static void main(String[] args) {
        String brokerList = "172.20.4.233:9092";
        String groupId = "mygroup";
        String topic = "topic201702241031";
        int workerNum = 3;



        ConsumerHandler consumerHandler = new ConsumerHandler(brokerList,groupId,topic);
        consumerHandler.execute(workerNum);
        try{
            Thread.sleep(100000);
        }catch (InterruptedException e){
             consumerHandler.shutdown();
        }

    }


}
