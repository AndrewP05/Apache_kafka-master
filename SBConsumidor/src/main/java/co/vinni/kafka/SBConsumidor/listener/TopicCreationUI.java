package co.vinni.kafka.SBConsumidor.listener;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import javax.swing.*;
import java.awt.*;
import java.util.Collections;
import java.util.Properties;

public class TopicCreationUI extends JFrame {

    private JTextField nombreTopicField;
    private JTextField particionesField;
    private JTextField replicacionField;
    private JButton crearButton;

    public TopicCreationUI() {
        super("Crear Tópico en Kafka");

        // Configuración básica (ajusta la conexión según corresponda)
        JPanel panel = new JPanel(new GridLayout(4, 2, 5, 5));
        panel.add(new JLabel("Nombre del Tópico:"));
        nombreTopicField = new JTextField(20);
        panel.add(nombreTopicField);

        panel.add(new JLabel("Particiones:"));
        particionesField = new JTextField(5);
        panel.add(particionesField);

        panel.add(new JLabel("Factor de replicación:"));
        replicacionField = new JTextField(5);
        panel.add(replicacionField);

        crearButton = new JButton("Crear Tópico");
        panel.add(new JLabel()); // Espacio vacío
        panel.add(crearButton);

        getContentPane().add(panel, BorderLayout.CENTER);
        setSize(400, 200);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        crearButton.addActionListener(e -> crearTopico());
    }

    private void crearTopico() {
        String nombreTopic = nombreTopicField.getText();
        int particiones;
        short factorReplicacion;

        try {
            particiones = Integer.parseInt(particionesField.getText());
            factorReplicacion = Short.parseShort(replicacionField.getText());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "Ingrese valores numéricos válidos para particiones y replicación", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (nombreTopic == null || nombreTopic.isEmpty()) {
            JOptionPane.showMessageDialog(this, "El nombre del tópico no puede estar vacío", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        // Configurar propiedades para el AdminClient
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Ajusta la dirección del servidor
        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(nombreTopic, particiones, factorReplicacion);
            // La creación se realiza de forma asíncrona
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            JOptionPane.showMessageDialog(this, "Tópico creado exitosamente");
        } catch (Exception ex) {
            ex.printStackTrace();
            JOptionPane.showMessageDialog(this, "Error al crear el tópico: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            TopicCreationUI ventanaTopic = new TopicCreationUI();
            ventanaTopic.setVisible(true);
        });
    }
}
