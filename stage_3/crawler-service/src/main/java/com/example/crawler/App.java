package com.example.crawler;

import com.google.gson.Gson;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Crawler Service (Stage 3)
 *
 * Responsabilidades:
 *  1. Leer la lista de BOOK_IDS desde variable de entorno.
 *  2. Descargar cada libro de Project Gutenberg (si no existe ya).
 *  3. Guardarlo en el datalake compartido (/datalake/YYYYMMDD/HH/).
 *  4. Publicar un evento JSON al topic "books.ingested" de ActiveMQ.
 *
 * Configuración (variables de entorno):
 *   NODE_ID      - identificador de esta instancia (ej: crawler1)
 *   BROKER_URL   - URL del broker ActiveMQ (ej: tcp://activemq:61616)
 *   DATALAKE_PATH - ruta del datalake compartido (ej: /datalake)
 *   BOOK_IDS     - lista separada por comas (ej: 1342,11,1513)
 */
public class App {

    private static final String START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK";
    private static final String END_MARKER   = "*** END OF THE PROJECT GUTENBERG EBOOK";
    private static final Gson   gson         = new Gson();

    public static void main(String[] args) throws Exception {
        String nodeId       = System.getenv().getOrDefault("NODE_ID",       "crawler1");
        String brokerUrl    = System.getenv().getOrDefault("BROKER_URL",    "tcp://localhost:61616");
        String datalakePath = System.getenv().getOrDefault("DATALAKE_PATH", "datalake");
        String bookIdsEnv   = System.getenv().getOrDefault("BOOK_IDS",      "1342");

        List<Integer> bookIds = Arrays.stream(bookIdsEnv.split(","))
            .map(String::trim)
            .map(Integer::parseInt)
            .toList();

        System.out.println("[" + nodeId + "] Iniciando crawler con libros: " + bookIds);

        // Conexión al broker ActiveMQ
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = ((ActiveMQConnectionFactory) factory).createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination topic = session.createTopic("books.ingested");
        MessageProducer producer = session.createProducer(topic);

        HttpClient http = HttpClient.newHttpClient();

        for (int bookId : bookIds) {
            System.out.println("[" + nodeId + "] Procesando libro " + bookId + "...");

            // Comprobar si ya fue descargado
            if (isAlreadyDownloaded(bookId, datalakePath)) {
                System.out.println("[" + nodeId + "] Libro " + bookId + " ya existe, publicando evento...");
                publishEvent(producer, session, topic, nodeId, bookId, findPath(bookId, datalakePath));
                continue;
            }

            // Descargar de Gutenberg
            String url = "https://www.gutenberg.org/cache/epub/" + bookId + "/pg" + bookId + ".txt";
            try {
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() != 200) {
                    System.err.println("[" + nodeId + "] Error HTTP " + resp.statusCode() + " para libro " + bookId);
                    continue;
                }

                String text = resp.body();
                if (!text.contains(START_MARKER) || !text.contains(END_MARKER)) {
                    System.err.println("[" + nodeId + "] Marcadores Gutenberg no encontrados en libro " + bookId);
                    continue;
                }

                // Separar header y body
                String[] parts = text.split(java.util.regex.Pattern.quote(START_MARKER), 2);
                String header = parts[0].strip();
                String[] bodyParts = parts[1].split(java.util.regex.Pattern.quote(END_MARKER), 2);
                String body = bodyParts[0].strip();

                // Guardar en datalake
                LocalDateTime now = LocalDateTime.now();
                String dateFolder = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String hourFolder = now.format(DateTimeFormatter.ofPattern("HH"));
                Path outputPath = Paths.get(datalakePath, dateFolder, hourFolder);
                Files.createDirectories(outputPath);
                Files.writeString(outputPath.resolve(bookId + ".body.txt"),   body,   StandardCharsets.UTF_8);
                Files.writeString(outputPath.resolve(bookId + ".header.txt"), header, StandardCharsets.UTF_8);

                System.out.println("[" + nodeId + "] Libro " + bookId + " guardado en " + outputPath);

                // Publicar evento al broker
                publishEvent(producer, session, topic, nodeId, bookId, outputPath.toString());

            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Error procesando libro " + bookId + ": " + e.getMessage());
            }
        }

        System.out.println("[" + nodeId + "] Todos los libros procesados. Crawler finalizado.");
        session.close();
        connection.close();
    }

    /**
     * Publica un evento JSON al topic "books.ingested".
     * Formato: {"bookId": 1342, "crawler": "crawler1", "path": "..."}
     */
    private static void publishEvent(MessageProducer producer, Session session,
                                      Destination topic, String nodeId, int bookId, String path)
            throws JMSException {
        String json = gson.toJson(java.util.Map.of(
            "bookId",  bookId,
            "crawler", nodeId,
            "path",    path,
            "status",  "READY"
        ));
        TextMessage msg = session.createTextMessage(json);
        // Usamos el bookId como correlationId para idempotencia en el indexer
        msg.setJMSCorrelationID("book-" + bookId);
        producer.send(msg);
        System.out.println("[" + nodeId + "] Evento publicado para libro " + bookId);
    }

    private static boolean isAlreadyDownloaded(int bookId, String datalakePath) {
        return !findBodyFiles(bookId, datalakePath).isEmpty();
    }

    private static String findPath(int bookId, String datalakePath) {
        List<Path> files = findBodyFiles(bookId, datalakePath);
        return files.isEmpty() ? "unknown" : files.get(0).getParent().toString();
    }

    private static List<Path> findBodyFiles(int bookId, String datalakePath) {
        Path root = Paths.get(datalakePath);
        if (!Files.exists(root)) return List.of();
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                .filter(p -> p.getFileName().toString().equals(bookId + ".body.txt"))
                .toList();
        } catch (IOException e) {
            return List.of();
        }
    }
}
