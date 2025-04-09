package co.vinni.kafka.SBConsumidor.config;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;
import java.util.Set;

public class KafkaUtils {

    public static String[] getTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Ajusta a tu entorno
        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> topicNames = adminClient.listTopics().names().get();
            return topicNames.toArray(new String[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return new String[]{};
        }
    }
}