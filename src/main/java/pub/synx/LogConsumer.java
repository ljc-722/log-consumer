package pub.synx;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class LogConsumer {
    // 接入点rmqproxy地址
    private final static String ENDPOINTS = "172.27.153.121:8081";
    // ClickHouse地址
    private final static String CLICKHOUSE_URL = "https://172.27.153.113:8123/";

    private final static String CONSUMER_GROUP = "TAH_GROUP";

    private final static String TOPIC = "TAH";

    private final static String TAG = "*";


    /**
     * 消息处理函数
     * @param messageView 来自RocketMQ的消息视图
     */
    public static ConsumeResult messageProcessor(MessageView messageView) {
        boolean isSuccess = false;
        ByteBuffer messageViewBody = messageView.getBody();
        ByteBuffer buffer = ByteBuffer.allocate(messageViewBody.remaining());
        buffer.put(messageViewBody).flip();
        String result = StandardCharsets.UTF_8.decode(buffer).toString();

        System.out.printf("Consume message, messageId=%s, body=%s%n", messageView.getMessageId(), result);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> map = null;
        try {
            map = objectMapper.readValue(result, Map.class);
            isSuccess = LogConsumer.store(map.get("timestamp"), map.get("log_level"), map.get("message"));
        } catch (IOException e) {
            System.out.printf("Failed to parse message: %s", e.getMessage());
        }
        if (isSuccess) {
            return ConsumeResult.SUCCESS;
        }
        return ConsumeResult.FAILURE;
    }


    /**
     * 日志存储到 clickhouse
     * @param timestamp 当前时间戳
     * @param logLevel 日志级别
     * @param message 日志内容
     */
    private static boolean store(String timestamp, String logLevel, String message) {
        String plainPayload = String.format("INSERT INTO `db_log`.`tbl_devices_log` (timestamp, log_level, message) " +
                "VALUES ('%s', '%s', '%s');", timestamp, logLevel, message);
        System.out.println("正在执行如下SQL：\n" + plainPayload);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost postRequest = new HttpPost(CLICKHOUSE_URL);
            postRequest.setHeader("Content-Type", "text/plain; charset=UTF-8");
            postRequest.setEntity(new StringEntity(plainPayload, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                // 打印 HTTP 状态码
                System.out.println("HTTP Status Code: " + response.getStatusLine().getStatusCode());
                // 打印响应体内容
                HttpEntity entity = response.getEntity();
                BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("HTTP Response Body: " + line);
                }
            }
            return true;
        } catch (Exception e) {
            System.out.printf("日志存储失败，错误信息为：%s", e.getMessage());
            return false;
        }
    }

    // 接收消息
    public static void receiveMessage() throws Exception {
        // 构建PushConsumer实例
        PushConsumer pushConsumer = ClientServiceProvider.loadService().newPushConsumerBuilder()
                .setClientConfiguration(ClientConfiguration.newBuilder().setEndpoints(ENDPOINTS).build())
                // 设置消费者分组。
                .setConsumerGroup(CONSUMER_GROUP)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC, new FilterExpression(TAG, FilterExpressionType.TAG)))
                // 设置消费监听器。
                .setMessageListener(LogConsumer::messageProcessor)
                .build();
        System.out.println("正在监听来自RocketMQ的消息...");
        // 保证进程不退出
        Thread.sleep(Long.MAX_VALUE);
        pushConsumer.close();
    }

    public static void main(String[] args) throws Exception {
        LogConsumer.receiveMessage();
    }
}

