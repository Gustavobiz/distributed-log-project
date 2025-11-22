package com.dist.replica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


/**
 * Nó de réplica simples (por enquanto, 1 líder).
 * Funcionalidades:
 *  - Key-Value Store em memória (/set e /get)
 *  - Envia REGISTER para o Gateway via UDP
 *  - Envia HEARTBEAT periódico para o Gateway via UDP
 */
public class ReplicaNodeApplication {

    private static final Map<String, String> STATE = new ConcurrentHashMap<>();

    //  NOVO: log replicado em memória
    private static final java.util.List<LogEntry> LOG =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    private static int lastAppliedIndex = 0;
    private static final java.util.concurrent.atomic.AtomicInteger LOG_INDEX_SEQ =
            new java.util.concurrent.atomic.AtomicInteger(0);

    //  Identidade e papel do nó
    private static String NODE_ID = "A1";
    private static String ROLE = "LEADER";

    //  Endereço HTTP do Gateway (para o líder mandar replicar)
    private static final String GATEWAY_BASE_URL = "http://localhost:8080";

    private static final String GATEWAY_HOST = "localhost";
    private static final int GATEWAY_UDP_PORT = 8000;

    // Entrada de log
    static class LogEntry {
        final int index;
        final String key;
        final String value;

        LogEntry(int index, String key, String value) {
            this.index = index;
            this.key = key;
            this.value = value;
        }
    }
    public static void main(String[] args) throws Exception {
        int port = 5000;
        NODE_ID = "A1";
        ROLE = "LEADER";

        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--nodeId=")) {
                NODE_ID = arg.substring("--nodeId=".length());
            } else if (arg.startsWith("--role=")) {
                ROLE = arg.substring("--role=".length());
            }
        }

        // Envia registro e inicia heartbeat
        sendRegister(NODE_ID, "localhost", port, ROLE);
        startHeartbeatThread(NODE_ID);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        System.out.println("[Nó " + NODE_ID + "] Servidor HTTP iniciado na porta " + port +
                " (papel=" + ROLE + ")");

        server.createContext("/set", new SetHandler());
        server.createContext("/get", new GetHandler());

        //  NOVO: endpoint interno para replicação de log
        server.createContext("/append", new AppendHandler());

        server.setExecutor(null);
        server.start();
    }


    // --------- REGISTRO E HEARTBEAT ---------

    private static void sendRegister(String id, String ip, int port, String role) {
        try {
            String msg = "REGISTER;" + id + ";" + ip + ";" + port + ";" + role;
            sendUdpMessage(msg);
            System.out.println("[Nó " + id + "] REGISTER enviado para o Gateway: " + msg);
        } catch (Exception e) {
            System.out.println("[Nó " + id + "] Erro ao enviar REGISTER: " + e.getMessage());
        }
    }

    private static void startHeartbeatThread(String nodeId) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    String msg = "HEARTBEAT;" + nodeId;
                    sendUdpMessage(msg);
                    // Descomente para ver cada heartbeat:
                    // System.out.println("[Nó " + nodeId + "] HEARTBEAT enviado");
                    Thread.sleep(2000);
                } catch (Exception e) {
                    System.out.println("[Nó " + nodeId + "] Erro no HEARTBEAT: " + e.getMessage());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private static void sendUdpMessage(String msg) throws IOException {
        byte[] data = msg.getBytes(StandardCharsets.UTF_8);
        try (DatagramSocket socket = new DatagramSocket()) {
            DatagramPacket packet = new DatagramPacket(
                    data, data.length,
                    InetAddress.getByName(GATEWAY_HOST),
                    GATEWAY_UDP_PORT
            );
            socket.send(packet);
        }
    }

    // --------- HANDLERS HTTP ---------
    static class SetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();

            if (query == null) {
                send(exchange, 400, "Faltando parâmetros ?key=&value=");
                return;
            }

            Map<String, String> params = QueryUtils.parseQuery(query);
            String key = params.get("key");
            String value = params.get("value");

            if (key == null || value == null) {
                send(exchange, 400, "Parâmetros 'key' ou 'value' ausentes");
                return;
            }

            System.out.println("[Nó " + NODE_ID + "] Recebeu SET key=" + key + " value=" + value);

            try {
                LogEntry entry = appendToLocalLog(key, value);
                applyEntry(entry);
                replicateEntryViaGateway(entry);

                send(exchange, 200, "OK (log index=" + entry.index + ")");
            } catch (Exception e) {
                e.printStackTrace();
                send(exchange, 500, "Erro ao processar SET com Log Replicado: " + e.getMessage());
            }
        }
    }



    static class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();

            if (query == null) {
                send(exchange, 400, "Faltando parâmetro ?key=");
                return;
            }

            Map<String, String> params = QueryUtils.parseQuery(query);
            String key = params.get("key");
           
            if (key == null) {
                send(exchange, 400, "Parâmetro 'key' ausente");
                return;
            }
             System.out.println("[Nó] Recebeu GET: key=" + key);

            String value = STATE.get(key);
            if (value == null) {
                send(exchange, 404, "Chave não encontrada");
            } else {
                send(exchange, 200, value);
            }
        }
    }
    // Handler chamado pelo Gateway para entregar entradas de Log aos followers
    static class AppendHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();

            if (query == null) {
                send(exchange, 400, "Faltando parâmetros ?index=&key=&value=");
                return;
            }

            Map<String, String> params = QueryUtils.parseQuery(query);
            String indexStr = params.get("index");
            String key = params.get("key");
            String value = params.get("value");

            if (indexStr == null || key == null || value == null) {
                send(exchange, 400, "Parâmetros 'index', 'key' ou 'value' ausentes");
                return;
            }

            int idx;
            try {
                idx = Integer.parseInt(indexStr);
            } catch (NumberFormatException e) {
                send(exchange, 400, "Índice inválido: " + indexStr);
                return;
            }

            LogEntry entry = new LogEntry(idx, key, value);
            synchronized (LOG) {
                LOG.add(entry);
            }
            applyEntry(entry);

            System.out.println("[Nó " + NODE_ID + "] APPEND recebido: index=" + idx +
                    " key=" + key + " value=" + value);

            send(exchange, 200, "OK APPEND index=" + idx);
        }
    }
    // ---- Funções do Log Replicado no Nó ----

    private static LogEntry appendToLocalLog(String key, String value) {
        int index = LOG_INDEX_SEQ.incrementAndGet();
        LogEntry entry = new LogEntry(index, key, value);
        synchronized (LOG) {
            LOG.add(entry);
        }
        System.out.println("[Nó " + NODE_ID + "] Log local: append index=" + index +
                " key=" + key + " value=" + value);
        return entry;
    }

    private static void applyEntry(LogEntry entry) {
        STATE.put(entry.key, entry.value);
        lastAppliedIndex = entry.index;
        System.out.println("[Nó " + NODE_ID + "] Estado aplicado: " +
                entry.key + "=" + entry.value + " (index=" + entry.index + ")");
    }

    private static void replicateEntryViaGateway(LogEntry entry) throws Exception {
        String url = GATEWAY_BASE_URL
                + "/append?index=" + entry.index
                + "&key=" + URLEncoder.encode(entry.key, StandardCharsets.UTF_8)
                + "&value=" + URLEncoder.encode(entry.value, StandardCharsets.UTF_8);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("[Nó " + NODE_ID + "] replicateEntryViaGateway -> " +
                "status=" + response.statusCode() +
                " body=" + response.body());
    }

    static class QueryUtils {
        static Map<String, String> parseQuery(String query) {
            Map<String, String> map = new ConcurrentHashMap<>();
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                if (!pair.contains("=")) continue;
                String[] kv = pair.split("=", 2);
                String k = java.net.URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
                String v = kv.length > 1 ? java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "";
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
