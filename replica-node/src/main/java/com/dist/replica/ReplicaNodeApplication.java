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

/**
 * Nó de réplica simples (por enquanto, 1 líder).
 * Funcionalidades:
 *  - Key-Value Store em memória (/set e /get)
 *  - Envia REGISTER para o Gateway via UDP
 *  - Envia HEARTBEAT periódico para o Gateway via UDP
 */
public class ReplicaNodeApplication {

    private static final Map<String, String> STATE = new ConcurrentHashMap<>();

    private static final String GATEWAY_HOST = "localhost";
    private static final int GATEWAY_UDP_PORT = 8000;

    public static void main(String[] args) throws Exception {
        int port = 5000;
        String nodeId = "A1";
        String role = "LEADER";

        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--nodeId=")) {
                nodeId = arg.substring("--nodeId=".length());
            } else if (arg.startsWith("--role=")) {
                role = arg.substring("--role=".length());
            }
        }

        // Envia registro e inicia heartbeat
        sendRegister(nodeId, "localhost", port, role);
        startHeartbeatThread(nodeId);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        System.out.println("[Nó " + nodeId + "] Servidor HTTP iniciado na porta " + port +
                " (papel=" + role + ")");

        server.createContext("/set", new SetHandler());
        server.createContext("/get", new GetHandler());

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
            System.out.println("[Nó] Recebeu SET: key=" + key + ", value=" + value);


            if (key == null || value == null) {
                send(exchange, 400, "Parâmetros 'key' ou 'value' ausentes");
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
