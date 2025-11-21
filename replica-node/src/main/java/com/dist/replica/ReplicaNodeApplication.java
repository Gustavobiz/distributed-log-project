package com.dist.replica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nó de réplica simples (por enquanto é só 1 nó líder).
 * Ele oferece:
 *  - /set?key=K&value=V
 *  - /get?key=K
 *
 * Tudo via HTTP.
 */
public class ReplicaNodeApplication {

    // Nosso key-value store em memória
    private static final Map<String, String> STATE = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {

        // Porta padrão
        int port = 5000;

        // Permite configurar via argumentos
        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            }
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        System.out.println("Replica Node HTTP running on port " + port);

        // Roteamento
        server.createContext("/set", new SetHandler());
        server.createContext("/get", new GetHandler());

        server.setExecutor(null);
        server.start();
    }

    // ------- HANDLERS -------

    static class SetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            String query = exchange.getRequestURI().getRawQuery();

            if (query == null) {
                send(exchange, 400, "Missing ?key=&value=");
                return;
            }

            Map<String, String> params = QueryUtils.parseQuery(query);

            String key = params.get("key");
            String value = params.get("value");

            if (key == null || value == null) {
                send(exchange, 400, "Missing key or value");
                return;
            }

            STATE.put(key, value);

            send(exchange, 200, "OK: set " + key + "=" + value);
        }
    }

    static class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            String query = exchange.getRequestURI().getRawQuery();

            if (query == null) {
                send(exchange, 400, "Missing ?key=");
                return;
            }

            Map<String, String> params = QueryUtils.parseQuery(query);
            String key = params.get("key");

            if (key == null) {
                send(exchange, 400, "Missing key");
                return;
            }

            String value = STATE.get(key);

            if (value == null) {
                send(exchange, 404, "Key not found");
            } else {
                send(exchange, 200, value);
            }
        }
    }

    // ------- HELPERS -------

    static class QueryUtils {
        static Map<String, String> parseQuery(String query) {
            Map<String, String> map = new ConcurrentHashMap<>();

            for (String pair : query.split("&")) {
                if (!pair.contains("=")) continue;

                String[] kv = pair.split("=", 2);
                String k = URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
                String v = kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "";

                map.put(k, v);
            }

            return map;
        }
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
