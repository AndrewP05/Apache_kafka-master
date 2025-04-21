package co.vinni.kafka.SBConsumidor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import co.vinni.kafka.SBConsumidor.config.KafkaUtils;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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

        topicComboBox = new JComboBox<>(KafkaUtils.getTopics());
        topicComboBox.addActionListener(e -> {
            detenerConsumo();
            areaMensajes.setText("");
            iniciarConsumo(getSelectedTopic());
        });

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
                        String mensajePlano = record.value();
                        String mensaje = "Tópico: " + topic
                                + " | Partición: " + record.partition()
                                + " | Offset: " + record.offset()
                                + " -> " + mensajePlano + "\n";

                        SwingUtilities.invokeLater(() -> areaMensajes.append(mensaje));

                        // Separar el mensaje con ';' para columnas
                        String[] campos = mensajePlano.split(";");
                        for (int i = 0; i < campos.length; i++) {
                            campos[i] = campos[i].trim();
                        }
                        guardarEnExcel(topic, campos);
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

    private void guardarEnExcel(String topic, String[] campos) {
        try {
            String archivo = "registros.xlsx";
            File file = new File(archivo);
            Workbook workbook;
            Sheet hoja;

            if (file.exists()) {
                FileInputStream fis = new FileInputStream(file);
                workbook = new XSSFWorkbook(fis);
                hoja = workbook.getSheet(topic);
                if (hoja == null) {
                    hoja = workbook.createSheet(topic);
                }
                fis.close();
            } else {
                workbook = new XSSFWorkbook();
                hoja = workbook.createSheet(topic);
            }

            int ultimaFila = hoja.getLastRowNum();
            if (ultimaFila == 0 && hoja.getRow(0) == null) {
                Row header = hoja.createRow(0);
                for (int i = 0; i < campos.length; i++) {
                    header.createCell(i).setCellValue("Campo " + (i + 1));
                }
            }

            Row fila = hoja.createRow(hoja.getLastRowNum() + 1);
            for (int i = 0; i < campos.length; i++) {
                fila.createCell(i).setCellValue(campos[i]);
            }

            FileOutputStream fos = new FileOutputStream(archivo);
            workbook.write(fos);
            fos.close();
            workbook.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
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
