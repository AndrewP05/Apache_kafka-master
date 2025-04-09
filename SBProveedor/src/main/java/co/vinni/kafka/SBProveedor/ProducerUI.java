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

        // Configuración del productor (puedes extraer esta configuración de tu proyecto)
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Ajusta según tu entorno
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

        // ComboBox con tópicos obtenidos de Kafka
        topicComboBox = new JComboBox<>(KafkaUtils.getTopics());
        
        // Botón para refrescar la lista de tópicos
        refrescarButton = new JButton("Refrescar Tópicos");
        refrescarButton.addActionListener((ActionEvent e) -> {
            String[] topics = KafkaUtils.getTopics();
            topicComboBox.setModel(new DefaultComboBoxModel<>(topics));
        });

        enviarButton.addActionListener(e -> {
            String mensaje = mensajeField.getText();
            String topic = topicComboBox.getSelectedItem().toString();
            if (mensaje != null && !mensaje.isEmpty()) {
                enviarMensaje(topic, mensaje);
                mensajeField.setText("");
            } else {
                JOptionPane.showMessageDialog(ProducerUI.this, "Ingrese un mensaje", "Error", JOptionPane.ERROR_MESSAGE);
            }
        });

        // Organización de los componentes en el panel
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);

        // Fila 0: Selección de tópico y botón de refrescar
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new JLabel("Tópico:"), gbc);
        gbc.gridx = 1;
        panel.add(topicComboBox, gbc);
        gbc.gridx = 2;
        panel.add(refrescarButton, gbc);

        // Fila 1: Campo de mensaje
        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new JLabel("Mensaje:"), gbc);
        gbc.gridx = 1;
        gbc.gridwidth = 2;
        panel.add(mensajeField, gbc);
        gbc.gridwidth = 1;

        // Fila 2: Botón de enviar
        gbc.gridx = 1;
        gbc.gridy = 2;
        panel.add(enviarButton, gbc);

        getContentPane().add(panel, BorderLayout.CENTER);
    }

    private void enviarMensaje(String topic, String mensaje) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, mensaje);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // Espera la confirmación
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