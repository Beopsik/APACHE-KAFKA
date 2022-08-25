package com.example.chapter4;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class FirstAppProducer {
    private static String topicName = "first-app";

    public static void main(String[] args) {

        /**
         * KafkaProducer에 필요한 설정
         * bootstrap.servers: 작성할 KafkaProducer가 접속하는 브로커의 호스트명과 포트 번호를 지정하고 있다.
         * key.serializer, value.serializer: 카프카에서는 모든 메시지가 직렬화된 상태로 전송된다.
         *                                    key.serializer와 value.serializer는 이 직렬화 처리에 이용되는 시리얼라이저 클래스를 지정한다. 
         */
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "kafka-broker01:9092,kafka-broker02:9092,kafka-broker03:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 카프카 클러스터에서 메시지를 송신(produce)하는 객체 생성
        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;
        for (int i = 0; i <= 100; i++) {
            key=i;
            value = String.valueOf(i);

            /**
             * 송신 메시지 생성
             * KafkaProducer를 이용하여 메시지를 보낼 때는 송신 메시지를 이 ProducerRecord라는 객체에 저장한다.
             * 이때 메시지의 Key, Value 외에 송신처의 토픽도 함께 등록한다.
             */
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);

            /**
             * 메시지를 송신하고 Ack를 받았을 때 실행할 작업(Callback) 등록
             * Callback 클래스에서 구현하고 있는 onCompletion 메서드에서는 송신을 완료했을 때 실행되어야 할 처리를 하고 있다.
             * KafkaProducer의 송신 처리는 비동기적으로 이루어지기 때문에 send 메서드를 호출했을 때 발생하지 않는다.
             * send 메서드의 처리는 KafkaProducer의 송신 큐에 메시지를 넣을 뿐이다.
             * 송신 큐에 넣은 메시지는 사용자의 애플리케이션과는 다른 별도의 스레드에서 순차적으로 송신된다.
             * 메시지가 송신된 경우 카프카 클러스터에서 Ack가 반환된다. Callback 클래스의 메서드는 그 Ack를 수신했을 때 처리된다.
             *
             */
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String infoString = String.format("Success partition:%d, offset:%d", recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        String infoString = String.format("Failed:%s", e.getMessage());

                        System.err.println(infoString);
                    }
                }
            });
        }

        producer.close();
    }
}