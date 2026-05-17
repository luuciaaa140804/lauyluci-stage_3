package com.example.crawler;

import com.google.gson.Gson;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

// NOTA: NO importamos javax.jms.* — usamos nombres completamente calificados
// para evitar cualquier colisión con otras clases del mismo nombre.

/**
 * Crawler Service (Stage 3) — con servidor REST y replicación HTTP entre peers
 *
 * Variables de entorno:
 *   NODE_ID       - identificador (ej: crawler1)
 *   BROKER_URL    - URL ActiveMQ (ej: tcp://activemq:61616)
 *   DATALAKE_PATH - ruta datalake (ej: /datalake)
 *   BOOK_IDS      - IDs separados por comas (ej: 1342,11,1513)
 *   PEER_URLS     - URLs de peers para replicación (ej: http://crawler2:8090)
 *   SERVER_PORT   - puerto REST de este nodo (default: 8090)
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
        String peerUrlsEnv  = System.getenv().getOrDefault("PEER_URLS",     "");
        int    serverPort   = Integer.parseInt(System.getenv().getOrDefault("SERVER_PORT", "8090"));

        List<Integer> bookIds = Arrays.stream(bookIdsEnv.split(","))
            .map(String::trim).filter(s -> !s.isEmpty())
            .map(Integer::parseInt).toList();

        List<String> peerUrls = Arrays.stream(peerUrlsEnv.split(","))
            .map(String::trim).filter(s -> !s.isEmpty()).toList();

        System.out.println("[" + nodeId + "] Iniciando crawler. Libros: " + bookIds);
        System.out.println("[" + nodeId + "] Peers: " + (peerUrls.isEmpty() ? "ninguno" : peerUrls));

        // Servidor REST para servir/recibir archivos
        startFileServer(nodeId, datalakePath, serverPort);

        // ActiveMQ — nombres completamente calificados
        javax.jms.ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        javax.jms.Connection jmsConn = factory.createConnection();
        jmsConn.start();
        javax.jms.Session jmsSession = jmsConn.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        javax.jms.Destination topic  = jmsSession.createTopic("books.ingested");
        javax.jms.MessageProducer producer = jmsSession.createProducer(topic);

        HttpClient http = HttpClient.newHttpClient();

        for (int bookId : bookIds) {
            System.out.println("[" + nodeId + "] Procesando libro " + bookId + "...");

            if (isAlreadyDownloaded(bookId, datalakePath)) {
                System.out.println("[" + nodeId + "] Libro " + bookId + " ya existe.");
                publishEvent(producer, jmsSession, nodeId, bookId,
                    findPath(bookId, datalakePath), peerUrls, serverPort);
                continue;
            }

            String url = "https://www.gutenberg.org/cache/epub/" + bookId + "/pg" + bookId + ".txt";
            try {
                HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() != 200) {
                    System.err.println("[" + nodeId + "] HTTP " + resp.statusCode() + " para libro " + bookId);
                    continue;
                }

                String text = resp.body();
                if (!text.contains(START_MARKER) || !text.contains(END_MARKER)) {
                    System.err.println("[" + nodeId + "] Marcadores no encontrados en libro " + bookId);
                    continue;
                }

                String[] parts  = text.split(java.util.regex.Pattern.quote(START_MARKER), 2);
                String   header = parts[0].strip();
                String   body   = parts[1].split(java.util.regex.Pattern.quote(END_MARKER), 2)[0].strip();

                // Guardar localmente
                LocalDateTime now = LocalDateTime.now();
                Path outputPath = Paths.get(datalakePath,
                    now.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                    now.format(DateTimeFormatter.ofPattern("HH")));
                Files.createDirectories(outputPath);
                Files.writeString(outputPath.resolve(bookId + ".body.txt"),   body,   StandardCharsets.UTF_8);
                Files.writeString(outputPath.resolve(bookId + ".header.txt"), header, StandardCharsets.UTF_8);
                System.out.println("[" + nodeId + "] Libro " + bookId + " guardado en " + outputPath);

                // Replicar a los peers
                replicateToPeers(http, nodeId, bookId, body, header, peerUrls);

                // Publicar evento al broker
                publishEvent(producer, jmsSession, nodeId, bookId,
                    outputPath.toString(), peerUrls, serverPort);

            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Error en libro " + bookId + ": " + e.getMessage());
            }
        }

        System.out.println("[" + nodeId + "] Crawler finalizado. Servidor REST activo para fallback.");
        jmsSession.close();
        jmsConn.close();
        Thread.currentThread().join();
    }

    // ── Servidor REST ─────────────────────────────────────────────────────────

    private static void startFileServer(String nodeId, String datalakePath, int port) {
        Javalin app = Javalin.create(config ->
            config.http.defaultContentType = "application/json"
        ).start(port);

        app.get("/status", ctx -> ctx.result(gson.toJson(Map.of(
            "node", nodeId, "service", "crawler", "status", "running"))));

        // Sirve body o header de un libro para fallback del indexer
        app.get("/raw/{bookId}/{type}", ctx -> {
            int    bookId = Integer.parseInt(ctx.pathParam("bookId"));
            String type   = ctx.pathParam("type");
            Path   file   = findFileInDatalake(bookId, type, datalakePath);
            if (file == null) {
                ctx.status(404).result(gson.toJson(Map.of("error", "not found")));
                return;
            }
            ctx.contentType("text/plain")
               .result(Files.readString(file, StandardCharsets.UTF_8));
        });

        // Recibe réplica de un peer
        app.put("/replicate/{bookId}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("bookId"));
            @SuppressWarnings("unchecked")
            Map<String, String> payload = gson.fromJson(ctx.body(), Map.class);

            LocalDateTime now = LocalDateTime.now();
            Path outPath = Paths.get(datalakePath,
                now.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                now.format(DateTimeFormatter.ofPattern("HH")));
            Files.createDirectories(outPath);
            Files.writeString(outPath.resolve(bookId + ".body.txt"),
                payload.getOrDefault("body",   ""), StandardCharsets.UTF_8);
            Files.writeString(outPath.resolve(bookId + ".header.txt"),
                payload.getOrDefault("header", ""), StandardCharsets.UTF_8);

            System.out.println("[" + nodeId + "] Réplica recibida: libro " + bookId);
            ctx.result(gson.toJson(Map.of("status", "replicated", "bookId", bookId)));
        });

        System.out.println("[" + nodeId + "] Servidor de archivos activo en puerto " + port);
    }

    // ── Replicación a peers ───────────────────────────────────────────────────

    private static void replicateToPeers(HttpClient http, String nodeId, int bookId,
                                          String body, String header, List<String> peerUrls) {
        if (peerUrls.isEmpty()) return;
        String payload = gson.toJson(Map.of("body", body, "header", header));

        for (String peer : peerUrls) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(peer + "/replicate/" + bookId))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("[" + nodeId + "] Replicado libro " + bookId
                    + " a " + peer + " -> HTTP " + resp.statusCode());
            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Fallo al replicar a " + peer
                    + " (libro " + bookId + "): " + e.getMessage());
            }
        }
    }

    // ── Publicación de evento ─────────────────────────────────────────────────

    private static void publishEvent(javax.jms.MessageProducer producer,
                                      javax.jms.Session session,
                                      String nodeId, int bookId, String path,
                                      List<String> peerUrls, int serverPort)
            throws javax.jms.JMSException {
        List<String> allUrls = new ArrayList<>();
        allUrls.add("http://" + nodeId + ":" + serverPort);
        allUrls.addAll(peerUrls);

        String json = gson.toJson(Map.of(
            "bookId",      bookId,
            "crawler",     nodeId,
            "path",        path,
            "status",      "READY",
            "crawlerUrls", allUrls
        ));
        javax.jms.TextMessage msg = session.createTextMessage(json);
        msg.setJMSCorrelationID("book-" + bookId);
        producer.send(msg);
        System.out.println("[" + nodeId + "] Evento publicado: libro " + bookId + " en " + allUrls);
    }

    // ── Utilidades datalake ───────────────────────────────────────────────────

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
            return stream.filter(p -> p.getFileName().toString()
                .equals(bookId + ".body.txt")).toList();
        } catch (IOException e) { return List.of(); }
    }

    private static Path findFileInDatalake(int bookId, String type, String datalakePath) {
        Path root = Paths.get(datalakePath);
        if (!Files.exists(root)) return null;
        try (Stream<Path> stream = Files.walk(root)) {
            return stream.filter(p -> p.getFileName().toString()
                .equals(bookId + "." + type + ".txt")).findFirst().orElse(null);
        } catch (IOException e) { return null; }
    }
}
