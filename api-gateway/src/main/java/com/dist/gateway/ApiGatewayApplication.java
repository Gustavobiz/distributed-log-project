package com.dist.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * API Gateway simples:
 * - Escuta HTTP na porta 8080
 * - Exponde /set e /get
 * - Encaminha as requisições para um nó líder HTTP (por enquanto fixo)
 *
 * Depois vamos:
 *  - adicionar ServiceRegistry
 *  - heartbeat via UDP
 *  - suporte a TCP/UDP
 *  - vários nós (leader/followers)
 */
public class ApiGatewayApplication {

    // URL base do nó líder (por enquanto fixa, depois vamos usar ServiceRegistry)
    private static String leaderBaseUrl = "http://localhost:5000";

    // Cliente HTTP para encaminhar as requisições
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {
        int port = 8080;

        // Permite configurar porta e URL do líder via argumentos
        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--leaderUrl=")) {
                leaderBaseUrl = arg.substring("--leaderUrl=".length());
            }
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        System.out.println("API Gateway HTTP running on port " + port);
        System.out.println("Forwarding to leader node at " + leaderBaseUrl);

        server.createContext("/set", new SetProxyHandler());
        server.createContext("/get", new GetProxyHandler());

        server.setExecutor(null);
        server.start();
    }

    // Handler para /set no Gateway
    static class SetProxyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();
            String targetUrl = leaderBaseUrl + "/set";
            if (query != null && !query.isEmpty()) {
                targetUrl += "?" + query;
            }

            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .GET()
                        .build();

                HttpResponse<String> response =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                send(exchange, response.statusCode(), response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                send(exchange, 500, "Interrupted: " + e.getMessage());
            } catch (Exception e) {
                send(exchange, 502, "Error forwarding to leader: " + e.getMessage());
            }
        }
    }

    // Handler para /get no Gateway
    static class GetProxyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getRawQuery();
            String targetUrl = leaderBaseUrl + "/get";
            if (query != null && !query.isEmpty()) {
                targetUrl += "?" + query;
            }

            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .GET()
                        .build();

                HttpResponse<String> response =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                send(exchange, response.statusCode(), response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                send(exchange, 500, "Interrupted: " + e.getMessage());
            } catch (Exception e) {
                send(exchange, 502, "Error forwarding to leader: " + e.getMessage());
            }
        }
    }

    // Envia resposta pro cliente
    private static void send(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
