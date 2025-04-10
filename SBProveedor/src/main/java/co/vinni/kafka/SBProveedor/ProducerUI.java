package co.vinni.kafka.SBProveedor;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import co.vinni.kafka.SBProveedor.config.KafkaUtils;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerUI extends JFrame {

    private JTextField mensajeField;
    private JButton enviarButton;
    private JComboBox<String> topicComboBox;
    private KafkaProducer<String, String> producer;
    private JButton refrescarButton;

    public ProducerUI() {
        super("Interfaz del Proveedor - Productor");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

        initComponents();
    }

    private void initComponents() {
        setSize(550, 180);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        mensajeField = new JTextField(30);
        enviarButton = new JButton("Enviar Mensaje");

        // Inicializamos el JComboBox con los tópicos actuales del clúster
        topicComboBox = new JComboBox<>(KafkaUtils.getTopics());
        
        // Botón para refrescar la lista de tópicos dinámicamente
        refrescarButton = new JButton("Refrescar Tópicos");
        refrescarButton.addActionListener((ActionEvent e) -> {
            String[] topics = KafkaUtils.getTopics();
            topicComboBox.setModel(new DefaultComboBoxModel<>(topics));
        });

        enviarButton.addActionListener(e -> {
            String mensaje = mensajeField.getText();
            if (mensaje != null && !mensaje.isEmpty() && topicComboBox.getItemCount() > 0) {
                String topic = topicComboBox.getSelectedItem().toString();
                enviarMensaje(topic, mensaje);
                mensajeField.setText("");
            } else {
                JOptionPane.showMessageDialog(ProducerUI.this, "Ingrese un mensaje y verifique que existan tópicos", "Error", JOptionPane.ERROR_MESSAGE);
            }
        });

        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new JLabel("Tópico:"), gbc);
        gbc.gridx = 1;
        panel.add(topicComboBox, gbc);
        gbc.gridx = 2;
        panel.add(refrescarButton, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new JLabel("Mensaje:"), gbc);
        gbc.gridx = 1;
        gbc.gridwidth = 2;
        panel.add(mensajeField, gbc);
        gbc.gridwidth = 1;

        gbc.gridx = 1;
        gbc.gridy = 2;
        panel.add(enviarButton, gbc);

        getContentPane().add(panel, BorderLayout.CENTER);
    }

    private void enviarMensaje(String topic, String mensaje) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, mensaje);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("Mensaje enviado a tópico: " + topic 
                    + " | Partición: " + metadata.partition() 
                    + " | Offset: " + metadata.offset());
        } catch (Exception ex) {
            ex.printStackTrace();
            JOptionPane.showMessageDialog(this, "Error al enviar el mensaje", "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    public void cerrar() {
        if (producer != null)
            producer.close();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            ProducerUI ventanaProducer = new ProducerUI();
            ventanaProducer.setVisible(true);
        });
    }
}