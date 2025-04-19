package co.vinni.kafka.SBProveedor;

import co.vinni.kafka.SBProveedor.config.KafkaUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerUI extends JFrame {
    private JComboBox<String> topicComboBox;
    private JPanel cardPanel;
    private CardLayout cardLayout;
    private JButton enviarButton;

    // Campos para 'estudiantes'
    private JTextField stNombre, stId, stCarrera, stSemestre, stMateria, stHora;
    // Campos para 'maestros'
    private JTextField mtNombre, mtId, mtDepartamento, mtMateria, mtOficina, mtHorario;

    private KafkaProducer<String,String> producer;

    public ProducerUI() {
        super("Proveedor");
        initProducer();
        initComponents();
    }

    private void createTopicsIfNeeded() {
    Properties adminProps = new Properties();
    adminProps.put("bootstrap.servers", "localhost:9092");

    try (AdminClient admin = AdminClient.create(adminProps)) {
        NewTopic estudiantes = new NewTopic("estudiantes", 1, (short) 1);
        NewTopic maestros = new NewTopic("maestros", 1, (short) 1);

        CreateTopicsResult result = admin.createTopics(java.util.List.of(estudiantes, maestros));
        result.all().get(); // Espera que termine

    } catch (TopicExistsException e) {
        System.out.println("Los tópicos ya existen.");
    } catch (Exception e) {
        e.printStackTrace();
        JOptionPane.showMessageDialog(this, "Error creando tópicos: " + e.getMessage(), "Kafka Error", JOptionPane.ERROR_MESSAGE);
    }
}

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

            // Crear los tópicos automáticamente
        createTopicsIfNeeded();
        producer = new KafkaProducer<>(props);
    }

    private void initComponents() {
        setSize(600, 360);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        // 1) ComboBox con tópicos
        topicComboBox = new JComboBox<>(KafkaUtils.getTopics());
        topicComboBox.addActionListener(e -> switchForm());

        // 2) Creamos los dos formularios

        // -- Formulario ESTUDIANTES --
        JPanel studentForm = new JPanel(new GridLayout(6, 2, 5, 5));
        stNombre   = new JTextField(); studentForm.add(new JLabel("Nombre:"));    studentForm.add(stNombre);
        stId       = new JTextField(); studentForm.add(new JLabel("ID:"));        studentForm.add(stId);
        stCarrera  = new JTextField(); studentForm.add(new JLabel("Carrera:"));   studentForm.add(stCarrera);
        stSemestre = new JTextField(); studentForm.add(new JLabel("Semestre:"));  studentForm.add(stSemestre);
        stMateria  = new JTextField(); studentForm.add(new JLabel("Materia:"));   studentForm.add(stMateria);
        stHora     = new JTextField(); studentForm.add(new JLabel("Hora:"));      studentForm.add(stHora);

        // -- Formulario MAESTROS --
        JPanel teacherForm = new JPanel(new GridLayout(6, 2, 5, 5));
        mtNombre       = new JTextField(); teacherForm.add(new JLabel("Nombre:"));      teacherForm.add(mtNombre);
        mtId           = new JTextField(); teacherForm.add(new JLabel("ID:"));          teacherForm.add(mtId);
        mtDepartamento = new JTextField(); teacherForm.add(new JLabel("Departamento:")); teacherForm.add(mtDepartamento);
        mtMateria      = new JTextField(); teacherForm.add(new JLabel("Materia:"));     teacherForm.add(mtMateria);
        mtOficina      = new JTextField(); teacherForm.add(new JLabel("Oficina:"));     teacherForm.add(mtOficina);
        mtHorario      = new JTextField(); teacherForm.add(new JLabel("Horario:"));     teacherForm.add(mtHorario);

        // 3) Panel con CardLayout
        cardLayout = new CardLayout();
        cardPanel = new JPanel(cardLayout);
        cardPanel.add(studentForm, "estudiantes");
        cardPanel.add(teacherForm, "maestros");

        // 4) Botón de enviar
        enviarButton = new JButton("Enviar");
        enviarButton.addActionListener(e -> onEnviar());
        
        

        // 5) Montaje de la UI
        JPanel north = new JPanel(new FlowLayout(FlowLayout.LEFT));
        north.add(new JLabel("Tópico:"));
        north.add(topicComboBox);

        getContentPane().add(north, BorderLayout.NORTH);
        getContentPane().add(cardPanel, BorderLayout.CENTER);
        getContentPane().add(enviarButton, BorderLayout.SOUTH);

        // Mostrar el formulario inicial
        switchForm();
    }

    private void switchForm() {
        String topic = (String) topicComboBox.getSelectedItem();
        if (topic != null) {
            cardLayout.show(cardPanel, topic);
        }
    }

    private void onEnviar() {
        String topic = (String) topicComboBox.getSelectedItem();
        String mensaje = "";

        if ("estudiantes".equals(topic)) {
            mensaje = stNombre.getText().trim()   + ";" +
                      stId.getText().trim()       + ";" +
                      stCarrera.getText().trim()  + ";" +
                      stSemestre.getText().trim() + ";" +
                      stMateria.getText().trim()  + ";" +
                      stHora.getText().trim();
                      
        } else if ("maestros".equals(topic)) {
            mensaje = mtNombre.getText().trim()       + ";" +
                      mtId.getText().trim()           + ";" +
                      mtDepartamento.getText().trim() + ";" +
                      mtMateria.getText().trim()      + ";" +
                      mtOficina.getText().trim()      + ";" +
                      mtHorario.getText().trim();
        }

        if (!mensaje.isEmpty()) {
            producer.send(new ProducerRecord<>(topic, mensaje));
            JOptionPane.showMessageDialog(this, "Enviado:\n" + mensaje);
            limpiarCampos();
        } else {
            JOptionPane.showMessageDialog(this, "Mensaje vacío, completa los campos.", "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void limpiarCampos() {
        String topic = ((String) topicComboBox.getSelectedItem()).trim().toLowerCase();
    
        if ("estudiantes".equals(topic)) {
            stNombre.setText("");
            stId.setText("");
            stCarrera.setText("");
            stSemestre.setText("");
            stMateria.setText("");
            stHora.setText("");
            stNombre.requestFocus();
        } else if ("maestros".equals(topic)) {
            mtNombre.setText("");
            mtId.setText("");
            mtDepartamento.setText("");
            mtMateria.setText("");
            mtOficina.setText("");
            mtHorario.setText("");
            mtNombre.requestFocus();
        }
    }



    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ProducerUI().setVisible(true));
    }
}