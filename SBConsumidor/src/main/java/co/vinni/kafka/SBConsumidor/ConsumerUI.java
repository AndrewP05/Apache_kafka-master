package co.vinni.kafka.SBConsumidor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import co.vinni.kafka.SBConsumidor.config.KafkaUtils;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class ConsumerUI extends JFrame {

    private JTextArea areaMensajes;
    private JComboBox<String> topicComboBox;
    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private JButton refrescarButton;

    public ConsumerUI() {
        super("Interfaz del Consumidor");
        initComponents();
        iniciarConsumo(getSelectedTopic());
    }

    private void initComponents() {
        setSize(650, 450);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        areaMensajes = new JTextArea();
        areaMensajes.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(areaMensajes);

        // Inicializamos con la lista dinámica de tópicos
        topicComboBox = new JComboBox<>(KafkaUtils.getTopics());
        topicComboBox.addActionListener(e -> {
            detenerConsumo();
            areaMensajes.setText("");
            iniciarConsumo(getSelectedTopic());
        });
        
        // Botón para refrescar la lista de tópicos dinámicamente
        refrescarButton = new JButton("Refrescar Tópicos");
        refrescarButton.addActionListener(e -> {
            String[] topics = KafkaUtils.getTopics();
            topicComboBox.setModel(new DefaultComboBoxModel<>(topics));
        });

        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        topPanel.add(new JLabel("Seleccionar Tópico:"));
        topPanel.add(topicComboBox);
        topPanel.add(refrescarButton);

        getContentPane().add(topPanel, BorderLayout.NORTH);
        getContentPane().add(scrollPane, BorderLayout.CENTER);
    }

    private String getSelectedTopic() {
        return topicComboBox.getSelectedItem().toString();
    }

    /**
     * Se asigna un group.id único para que cada consumidor actúe de forma independiente y reciba
     * cada mensaje publicado en el tópico.
     */
    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "grupo_consumidor_" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private void iniciarConsumo(String topic) {
        consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Arrays.asList(topic));

        consumerThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String mensaje = "Tópico: " + topic 
                                + " | Partición: " + record.partition() 
                                + " | Offset: " + record.offset() 
                                + " -> " + record.value() + "\n";
                        SwingUtilities.invokeLater(() -> areaMensajes.append(mensaje));
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                consumer.close();
            }
        });
        consumerThread.start();
    }

    private void detenerConsumo() {
        if (consumerThread != null && consumerThread.isAlive()) {
            consumerThread.interrupt();
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void cerrar() {
        detenerConsumo();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            ConsumerUI ventanaConsumer = new ConsumerUI();
            ventanaConsumer.setVisible(true);
        });
    }
}