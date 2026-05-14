package com.example.indexing;

import com.google.gson.Gson;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.multimap.MultiMap;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

/**
 * Indexing Service (Stage 3)
 *
 * Responsabilidades:
 *  1. Conectarse al clúster Hazelcast (índice invertido en memoria).
 *  2. Suscribirse al topic "books.ingested" de ActiveMQ.
 *  3. Por cada evento recibido: extraer metadatos + tokenizar body
 *     + actualizar el MultiMap de Hazelcast.
 *  4. Garantizar idempotencia: no indexar dos veces el mismo libro.
 *
 * Configuración (variables de entorno):
 *   NODE_ID          - identificador de esta instancia (ej: indexer1)
 *   BROKER_URL       - URL de ActiveMQ (ej: tcp://activemq:61616)
 *   DATALAKE_PATH    - ruta del datalake (ej: /datalake)
 *   DB_PATH          - ruta de la base de datos SQLite (ej: /datamarts/metadata.db)
 *   HZ_CLUSTER_NAME  - nombre del clúster Hazelcast (ej: search-cluster)
 */
public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        String nodeId      = System.getenv().getOrDefault("NODE_ID",         "indexer1");
        String brokerUrl   = System.getenv().getOrDefault("BROKER_URL",      "tcp://localhost:61616");
        String datalake    = System.getenv().getOrDefault("DATALAKE_PATH",   "datalake");
        String dbPath      = System.getenv().getOrDefault("DB_PATH",         "datamarts/metadata.db");
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER_NAME", "search-cluster");

        System.out.println("[" + nodeId + "] Iniciando Indexing Service...");

        // 1. Inicializar base de datos SQLite
        initDatabase(dbPath);

        // 2. Conectarse al clúster Hazelcast
        HazelcastInstance hz = createHazelcastMember(clusterName);
        MultiMap<String, Integer> invertedIndex = hz.getMultiMap("inverted-index");
        System.out.println("[" + nodeId + "] Conectado a Hazelcast. Particiones: "
            + hz.getPartitionService().getPartitions().size());

        // Set distribuido para rastrear libros ya indexados (idempotencia)
        com.hazelcast.collection.ISet<Integer> indexedBooks = hz.getSet("indexed-books");

        // 3. Conectarse al broker y suscribirse al topic
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = ((ActiveMQConnectionFactory) factory).createConnection();
        connection.setClientID(nodeId);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Usamos Topic con suscripción duradera para no perder mensajes si el indexer se reinicia
        Topic topic = session.createTopic("books.ingested");
        MessageConsumer consumer = session.createConsumer(topic);

        System.out.println("[" + nodeId + "] Esperando mensajes del broker...");

        // 4. Procesar mensajes de forma continua
        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage textMsg) {
                    String json = textMsg.getText();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> event = gson.fromJson(json, Map.class);

                    int bookId = ((Double) event.get("bookId")).intValue();

                    // Idempotencia: si ya fue indexado, ignoramos el mensaje
                    if (indexedBooks.contains(bookId)) {
                        System.out.println("[" + nodeId + "] Libro " + bookId + " ya indexado, ignorando.");
                        return;
                    }

                    System.out.println("[" + nodeId + "] Indexando libro " + bookId + "...");
                    indexBook(bookId, datalake, dbPath, invertedIndex);
                    indexedBooks.add(bookId);
                    System.out.println("[" + nodeId + "] Libro " + bookId + " indexado correctamente.");
                }
            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Error procesando mensaje: " + e.getMessage());
            }
        });

        // Mantener el servicio corriendo indefinidamente
        Thread.currentThread().join();
    }

    // ── Hazelcast ─────────────────────────────────────────────────────────────

    /**
     * Crea un miembro (nodo completo) del clúster Hazelcast.
     * Usa multicast UDP para descubrimiento automático dentro de la red Docker.
     */
    private static HazelcastInstance createHazelcastMember(String clusterName) {
        Config config = new Config();
        config.setClusterName(clusterName);

        // Puerto base; autoincrement permite múltiples nodos en la misma máquina
        config.getNetworkConfig()
              .setPort(5701)
              .setPortAutoIncrement(true);

        // Descubrimiento por multicast (funciona en redes Docker bridge)
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(true);
        joinConfig.getTcpIpConfig().setEnabled(false);

        // Configuración del MultiMap del índice invertido
        // backupCount=1: cada partición tiene 1 réplica -> tolera 1 fallo de nodo
        MultiMapConfig mmConfig = new MultiMapConfig("inverted-index")
            .setBackupCount(1)
            .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        config.addMultiMapConfig(mmConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    // ── Indexación ────────────────────────────────────────────────────────────

    private static void indexBook(int bookId, String datalakePath, String dbPath,
                                   MultiMap<String, Integer> index) throws Exception {
        // Buscar archivos en el datalake
        Path bodyFile   = findFile(bookId, "body",   datalakePath);
        Path headerFile = findFile(bookId, "header", datalakePath);

        if (bodyFile == null || headerFile == null) {
            throw new Exception("Archivos no encontrados para libro " + bookId);
        }

        // Extraer y guardar metadatos
        String header = Files.readString(headerFile, StandardCharsets.UTF_8);
        Map<String, String> meta = extractMetadata(header);
        saveMetadata(bookId, meta, dbPath);

        // Tokenizar body y actualizar índice en Hazelcast
        String body  = Files.readString(bodyFile, StandardCharsets.UTF_8);
        Set<String> words = tokenize(body);

        for (String word : words) {
            index.put(word, bookId);
        }
    }

    private static Set<String> tokenize(String text) {
        Set<String> words = new HashSet<>();
        Matcher m = Pattern.compile("\\b\\w+\\b").matcher(text.toLowerCase());
        while (m.find()) words.add(m.group());
        return words;
    }

    private static Map<String, String> extractMetadata(String header) {
        return Map.of(
            "title",    extract(header, "Title"),
            "author",   extract(header, "Author"),
            "language", extract(header, "Language"),
            "year",     extractYear(header)
        );
    }

    private static String extract(String text, String field) {
        Matcher m = Pattern.compile("^" + field + ":\\s*(.+)$", Pattern.MULTILINE).matcher(text);
        return m.find() ? m.group(1).strip() : "Unknown";
    }

    private static String extractYear(String text) {
        Matcher m = Pattern.compile("Release Date:.*?(\\d{4})", Pattern.CASE_INSENSITIVE).matcher(text);
        return m.find() ? m.group(1) : "Unknown";
    }

    // ── SQLite ────────────────────────────────────────────────────────────────

    private static void initDatabase(String dbPath) throws SQLException {
        Files.createDirectories(Paths.get(dbPath).getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS books (
                    book_id  INTEGER PRIMARY KEY,
                    title    TEXT,
                    author   TEXT,
                    language TEXT,
                    year     TEXT
                )
            """);
        } catch (Exception e) {
            System.err.println("[Indexer] Error iniciando BD: " + e.getMessage());
        }
    }

    private static void saveMetadata(int bookId, Map<String, String> meta, String dbPath) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO books (book_id, title, author, language, year) VALUES (?,?,?,?,?)")) {
            ps.setInt(1, bookId);
            ps.setString(2, meta.getOrDefault("title",    "Unknown"));
            ps.setString(3, meta.getOrDefault("author",   "Unknown"));
            ps.setString(4, meta.getOrDefault("language", "Unknown"));
            ps.setString(5, meta.getOrDefault("year",     "Unknown"));
            ps.executeUpdate();
        }
    }

    // ── Utilidades datalake ───────────────────────────────────────────────────

    private static Path findFile(int bookId, String type, String datalakePath) throws Exception {
        Path root = Paths.get(datalakePath);
        if (!Files.exists(root)) return null;
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                .filter(p -> p.getFileName().toString().equals(bookId + "." + type + ".txt"))
                .findFirst().orElse(null);
        }
    }
}
