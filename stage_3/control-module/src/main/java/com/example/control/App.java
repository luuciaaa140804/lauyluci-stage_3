package com.example.control;

import com.google.gson.Gson;
import io.javalin.Javalin;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

/**
 * Control Module (Stage 3)
 *
 * API de administración del clúster. Permite:
 *  - Verificar el estado de todos los servicios
 *  - Disparar reindexaciones mediante eventos al broker
 *  - Consultar estadísticas del sistema
 *
 * Endpoints:
 *   GET  /status              -> estado del módulo de control
 *   GET  /cluster/status      -> estado del clúster completo
 *   POST /cluster/reindex     -> publica evento reindex.request al broker
 *   GET  /search?q={term}     -> proxy a la búsqueda (a través del Load Balancer)
 *
 * Configuración (variables de entorno):
 *   BROKER_URL  - URL de ActiveMQ (ej: tcp://activemq:61616)
 *   SEARCH_URL  - URL del Load Balancer (ej: http://nginx:80)
 */
public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        String brokerUrl = System.getenv().getOrDefault("BROKER_URL", "tcp://localhost:61616");
        String searchUrl = System.getenv().getOrDefault("SEARCH_URL", "http://localhost:80");

        System.out.println("[Control] Iniciando Control Module en puerto 7000...");

        // Conexión al broker para publicar eventos de reindexación
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection brokerConn = ((ActiveMQConnectionFactory) factory).createConnection();
        brokerConn.start();
        Session session = brokerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination reindexTopic = session.createTopic("reindex.request");
        MessageProducer producer = session.createProducer(reindexTopic);

        HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(7000);

        // Health check del propio módulo de control
        app.get("/status", ctx -> {
            ctx.result(gson.toJson(Map.of(
                "service", "control-module",
                "status",  "running",
                "broker",  brokerUrl,
                "search",  searchUrl
            )));
        });

        // Estado del clúster: ping a todos los search nodes a través del LB
        app.get("/cluster/status", ctx -> {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("load_balancer", checkService(http, searchUrl + "/status"));
            result.put("broker_url",    brokerUrl);
            result.put("timestamp",     java.time.Instant.now().toString());
            ctx.result(gson.toJson(result));
        });

        // Disparar una reindexación global publicando un evento al broker
        app.post("/cluster/reindex", ctx -> {
            String json = gson.toJson(Map.of(
                "event",     "reindex.request",
                "timestamp", java.time.Instant.now().toString()
            ));
            TextMessage msg = session.createTextMessage(json);
            producer.send(reindexTopic, msg);
            ctx.result(gson.toJson(Map.of("status", "reindex event published")));
        });

        // Proxy de búsqueda: reenvía la query al Load Balancer
        app.get("/search", ctx -> {
            String q        = ctx.queryParam("q");
            String author   = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String year     = ctx.queryParam("year");

            if (q == null || q.isBlank()) {
                ctx.status(400).result(gson.toJson(Map.of("error", "El parámetro 'q' es obligatorio")));
                return;
            }

            StringBuilder urlBuilder = new StringBuilder(searchUrl + "/search?q=" + q);
            if (author   != null) urlBuilder.append("&author=").append(author);
            if (language != null) urlBuilder.append("&language=").append(language);
            if (year     != null) urlBuilder.append("&year=").append(year);

            try {
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.status(resp.statusCode()).result(resp.body());
            } catch (Exception e) {
                ctx.status(503).result(gson.toJson(Map.of("error", e.getMessage())));
            }
        });

        System.out.println("[Control] Control Module listo en http://0.0.0.0:7000");
        Thread.currentThread().join();
    }

    private static Map<String, Object> checkService(HttpClient http, String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            @SuppressWarnings("unchecked")
            Map<String, Object> body = new Gson().fromJson(resp.body(), Map.class);
            return body != null ? body : Map.of("status", "unknown");
        } catch (Exception e) {
            return Map.of("status", "unreachable", "error", e.getMessage());
        }
    }
}
