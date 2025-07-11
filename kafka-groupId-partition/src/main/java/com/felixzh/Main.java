package com.felixzh;

/**
 * @author felixzh
 * <p>
 * 消费者提交offset到Kafka的内部Topic： __consumer_offset，同一个消费者组groupID只能写一个分区，因为需要保序。
 * 至于写哪个分区通过如下方式计算。
 */
public class Main {
    public static void main(String[] args) {
        // Kafka源码 GroupMetadataManager.scala
        // def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
        System.out.println(abs("console-consumer-46999".hashCode()) % 50);
        System.out.println(abs("f2".hashCode()) % 50);

        // 8 -> console-consumer-46999

        /**
         *
         * [root@felixzh bin]# ./kafka-console-consumer.sh --topic __consumer_offsets --partition 8 --bootstrap-server felixzh:6667 --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --from-beginning
         * [console-consumer-46999,json_topic,0]::OffsetAndMetadata(offset=1, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682382249810, expireTimestamp=Some(6866382249810))
         * [console-consumer-46999,json_topic,1]::OffsetAndMetadata(offset=3, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682382249810, expireTimestamp=Some(6866382249810))
         * [console-consumer-46999,json_topic,2]::OffsetAndMetadata(offset=4, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682382249810, expireTimestamp=Some(6866382249810))
         * [console-consumer-8010,json_topic,0]::OffsetAndMetadata(offset=1, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682423022004, expireTimestamp=Some(6866423022004))
         * [console-consumer-8010,json_topic,1]::OffsetAndMetadata(offset=3, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682423022004, expireTimestamp=Some(6866423022004))
         * [console-consumer-8010,json_topic,2]::OffsetAndMetadata(offset=4, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1682423022004, expireTimestamp=Some(6866423022004))
         * ^CProcessed a total of 10 messages
         *
         * */
    }

    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }
}