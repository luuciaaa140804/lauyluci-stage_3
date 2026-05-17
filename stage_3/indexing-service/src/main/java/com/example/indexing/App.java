package com.example.indexing;

import com.google.gson.Gson;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.multimap.MultiMap;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

// NOTA: NO importamos javax.jms.* para evitar colisión con java.sql.Connection
// Usamos nombres completamente calificados: javax.jms.Connection, javax.jms.Session, etc.

/**
 * Indexing Service (Stage 3)
 *
 * - Conecta a Hazelcast como miembro del clúster
 * - Suscribe al topic "books.ingested" de ActiveMQ
 * - Indexa cada libro: lee del datalake o hace fallback HTTP a los crawlers
 * - Garantiza idempotencia con un ISet distribuido
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

        initDatabase(dbPath);

        // Hazelcast — índice en memoria distribuido
        HazelcastInstance hz = createHazelcastMember(clusterName);
        MultiMap<String, Integer> invertedIndex = hz.getMultiMap("inverted-index");
        com.hazelcast.collection.ISet<Integer> indexedBooks = hz.getSet("indexed-books");
        System.out.println("[" + nodeId + "] Hazelcast conectado. Miembros: "
            + hz.getCluster().getMembers().size());

        // HTTP client para fallback entre crawlers
        HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

        // ActiveMQ — usando nombres completamente calificados para evitar ambigüedad con java.sql
        javax.jms.ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        javax.jms.Connection jmsConn = factory.createConnection();
        jmsConn.setClientID(nodeId);
        jmsConn.start();
        javax.jms.Session jmsSession = jmsConn.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic topic = jmsSession.createTopic("books.ingested");
        javax.jms.MessageConsumer consumer = jmsSession.createConsumer(topic);

        System.out.println("[" + nodeId + "] Esperando mensajes del broker...");

        consumer.setMessageListener(message -> {
            try {
                if (!(message instanceof javax.jms.TextMessage textMsg)) return;

                @SuppressWarnings("unchecked")
                Map<String, Object> event = gson.fromJson(textMsg.getText(), Map.class);
                int bookId = ((Double) event.get("bookId")).intValue();

                // Idempotencia
                if (indexedBooks.contains(bookId)) {
                    System.out.println("[" + nodeId + "] Libro " + bookId + " ya indexado, ignorando.");
                    return;
                }

                @SuppressWarnings("unchecked")
                List<String> crawlerUrls = (List<String>) event.getOrDefault("crawlerUrls", List.of());

                System.out.println("[" + nodeId + "] Indexando libro " + bookId + "...");
                indexBook(bookId, datalake, dbPath, invertedIndex, crawlerUrls, http, nodeId);
                indexedBooks.add(bookId);
                System.out.println("[" + nodeId + "] Libro " + bookId + " indexado OK.");

            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Error procesando mensaje: " + e.getMessage());
            }
        });

        Thread.currentThread().join();
    }

    // ── Indexación con fallback HTTP ──────────────────────────────────────────

    private static void indexBook(int bookId, String datalakePath, String dbPath,
                                   MultiMap<String, Integer> index, List<String> crawlerUrls,
                                   HttpClient http, String nodeId) throws Exception {
        // 1. Intentar leer desde el volumen compartido
        String body   = readFromDatalake(bookId, "body",   datalakePath);
        String header = readFromDatalake(bookId, "header", datalakePath);

        // 2. Fallback HTTP a los crawlers si no está en el volumen
        if (body == null || header == null) {
            System.out.println("[" + nodeId + "] Libro " + bookId
                + " no en datalake local, intentando fallback HTTP...");
            for (String crawlerUrl : crawlerUrls) {
                try {
                    if (body   == null) body   = fetchFromCrawler(http, crawlerUrl, bookId, "body");
                    if (header == null) header = fetchFromCrawler(http, crawlerUrl, bookId, "header");
                    if (body != null && header != null) {
                        System.out.println("[" + nodeId + "] Contenido obtenido de " + crawlerUrl);
                        break;
                    }
                } catch (Exception e) {
                    System.err.println("[" + nodeId + "] Fallback a " + crawlerUrl + " fallido: " + e.getMessage());
                }
            }
        }

        if (body == null || header == null) {
            throw new Exception("No se pudo obtener libro " + bookId + " de ninguna fuente.");
        }

        // 3. Metadatos → SQLite
        Map<String, String> meta = extractMetadata(header);
        saveMetadata(bookId, meta, dbPath);

        // 4. Tokenizar → Hazelcast
        Set<String> words = tokenize(body);
        for (String word : words) index.put(word, bookId);
        System.out.println("[" + nodeId + "] " + words.size() + " palabras indexadas para libro " + bookId);
    }

    private static String readFromDatalake(int bookId, String type, String datalakePath) {
        Path root = Paths.get(datalakePath);
        if (!Files.exists(root)) return null;
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                .filter(p -> p.getFileName().toString().equals(bookId + "." + type + ".txt"))
                .findFirst()
                .map(p -> { try { return Files.readString(p, StandardCharsets.UTF_8); }
                            catch (Exception e) { return null; } })
                .orElse(null);
        } catch (Exception e) { return null; }
    }

    private static String fetchFromCrawler(HttpClient http, String crawlerUrl, int bookId, String type) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(crawlerUrl + "/raw/" + bookId + "/" + type))
                .timeout(Duration.ofSeconds(10))
                .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 ? resp.body() : null;
        } catch (Exception e) { return null; }
    }

    // ── Hazelcast ─────────────────────────────────────────────────────────────

    private static HazelcastInstance createHazelcastMember(String clusterName) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPort(5701).setPortAutoIncrement(true);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(true);
        join.getTcpIpConfig().setEnabled(false);

        config.addMultiMapConfig(new MultiMapConfig("inverted-index")
            .setBackupCount(1)
            .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET));

        return Hazelcast.newHazelcastInstance(config);
    }

    // ── Tokenización y metadatos ──────────────────────────────────────────────

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

    // ── SQLite — usa java.sql.* explícito para evitar cualquier ambigüedad ────

    private static void initDatabase(String dbPath) {
        try { Files.createDirectories(Paths.get(dbPath).getParent()); } catch (Exception ignored) {}
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS books (" +
                "book_id INTEGER PRIMARY KEY, title TEXT, author TEXT, language TEXT, year TEXT)");
        } catch (Exception e) {
            System.err.println("[Indexer] Error BD: " + e.getMessage());
        }
    }

    private static void saveMetadata(int bookId, Map<String, String> meta, String dbPath)
            throws java.sql.SQLException {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             java.sql.PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO books (book_id, title, author, language, year) VALUES (?,?,?,?,?)")) {
            ps.setInt(1, bookId);
            ps.setString(2, meta.getOrDefault("title",    "Unknown"));
            ps.setString(3, meta.getOrDefault("author",   "Unknown"));
            ps.setString(4, meta.getOrDefault("language", "Unknown"));
            ps.setString(5, meta.getOrDefault("year",     "Unknown"));
            ps.executeUpdate();
        }
    }
}
