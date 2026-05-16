package com.example.search;

import com.google.gson.Gson;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.multimap.MultiMap;
import io.javalin.Javalin;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Search Service (Stage 3)
 *
 * Responsabilidades:
 *  1. Unirse al clúster Hazelcast como miembro (tiene una copia del índice).
 *  2. Exponer una API REST en el puerto 8080.
 *  3. Responder queries consultando el MultiMap distribuido de Hazelcast.
 *  4. Enriquecer resultados con metadatos de SQLite.
 *  5. Aplicar filtros opcionales (author, language, year).
 *
 * Configuración (variables de entorno):
 *   NODE_ID         - identificador de esta instancia (ej: search1)
 *   HZ_CLUSTER_NAME - nombre del clúster Hazelcast (ej: search-cluster)
 *   DB_PATH         - ruta de SQLite (ej: /datamarts/metadata.db)
 */
public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        String nodeId      = System.getenv().getOrDefault("NODE_ID",         "search1");
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER_NAME", "search-cluster");
        String dbPath      = System.getenv().getOrDefault("DB_PATH",         "datamarts/metadata.db");

        System.out.println("[" + nodeId + "] Iniciando Search Service...");

        // Conectarse al clúster Hazelcast como miembro completo
        HazelcastInstance hz = createHazelcastMember(clusterName);
        MultiMap<String, Integer> invertedIndex = hz.getMultiMap("inverted-index");

        System.out.println("[" + nodeId + "] Conectado a Hazelcast como miembro del clúster.");

        // Servidor Javalin en puerto 8080
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(8080);

        // Health check — Nginx lo usa para detectar nodos caídos
        app.get("/status", ctx -> {
            ctx.result(gson.toJson(Map.of(
                "service", "search-service",
                "node",    nodeId,
                "status",  "running",
                "hz_members", hz.getCluster().getMembers().size()
            )));
        });

        // Endpoint de búsqueda principal
        app.get("/search", ctx -> {
            String query    = ctx.queryParam("q");
            String author   = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String year     = ctx.queryParam("year");

            if (query == null || query.isBlank()) {
                ctx.status(400).result(gson.toJson(Map.of("error", "El parámetro 'q' es obligatorio")));
                return;
            }

            long start = System.currentTimeMillis();

            // 1. Buscar en el índice Hazelcast
            Collection<Integer> bookIds = invertedIndex.get(query.toLowerCase().strip());

            // 2. Recuperar metadatos de SQLite
            List<Map<String, Object>> results = fetchMetadata(new ArrayList<>(bookIds), dbPath);

            // 3. Aplicar filtros
            if (author != null && !author.isBlank()) {
                String f = author.toLowerCase();
                results = results.stream()
                    .filter(r -> String.valueOf(r.get("author")).toLowerCase().contains(f))
                    .collect(Collectors.toList());
            }
            if (language != null && !language.isBlank()) {
                String f = language.toLowerCase();
                results = results.stream()
                    .filter(r -> String.valueOf(r.get("language")).toLowerCase().contains(f))
                    .collect(Collectors.toList());
            }
            if (year != null && !year.isBlank()) {
                results = results.stream()
                    .filter(r -> String.valueOf(r.get("year")).contains(year))
                    .collect(Collectors.toList());
            }

            long latency = System.currentTimeMillis() - start;

            // Construir respuesta
            Map<String, String> filters = new LinkedHashMap<>();
            if (author   != null && !author.isBlank())   filters.put("author",   author);
            if (language != null && !language.isBlank()) filters.put("language", language);
            if (year     != null && !year.isBlank())     filters.put("year",     year);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("query",      query);
            response.put("filters",    filters);
            response.put("count",      results.size());
            response.put("latency_ms", latency);
            response.put("served_by",  nodeId);        // útil para ver el balanceo
            response.put("results",    results);

            ctx.result(gson.toJson(response));
        });

        System.out.println("[" + nodeId + "] Search Service listo en http://0.0.0.0:8080");
    }

    // ── Hazelcast ─────────────────────────────────────────────────────────────

    private static HazelcastInstance createHazelcastMember(String clusterName) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig()
              .setPort(5701)
              .setPortAutoIncrement(true);

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(true);
        joinConfig.getTcpIpConfig().setEnabled(false);

        MultiMapConfig mmConfig = new MultiMapConfig("inverted-index")
            .setBackupCount(1)
            .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        config.addMultiMapConfig(mmConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    // ── SQLite ────────────────────────────────────────────────────────────────

    private static List<Map<String, Object>> fetchMetadata(List<Integer> bookIds, String dbPath) {
        List<Map<String, Object>> results = new ArrayList<>();
        if (bookIds.isEmpty()) return results;

        String placeholders = bookIds.stream().map(id -> "?").collect(Collectors.joining(", "));
        String sql = "SELECT book_id, title, author, language, year FROM books WHERE book_id IN (" + placeholders + ")";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < bookIds.size(); i++) ps.setInt(i + 1, bookIds.get(i));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> book = new LinkedHashMap<>();
                book.put("book_id",  rs.getInt("book_id"));
                book.put("title",    rs.getString("title"));
                book.put("author",   rs.getString("author"));
                book.put("language", rs.getString("language"));
                book.put("year",     rs.getString("year"));
                results.add(book);
            }
        } catch (SQLException e) {
            System.err.println("[Search] Error consultando BD: " + e.getMessage());
        }
        return results;
    }
}
