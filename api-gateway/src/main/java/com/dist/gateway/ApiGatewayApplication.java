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
import java.util.List;

/**
 * API Gateway:
 *  - HTTP na porta 8080 (/set e /get)
 *  - UDP na porta 8000 (REGISTER + HEARTBEAT)
 */
public class ApiGatewayApplication {

    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {
        int httpPort = 8080;
        int udpPort = 8000;

        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                httpPort = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--udpPort=")) {
                udpPort = Integer.parseInt(arg.substring("--udpPort=".length()));
            }
        }

        // Inicia o servidor UDP para REGISTER + HEARTBEAT
        Thread udpThread = new Thread(new UDPRegisterServer(udpPort));
        udpThread.setDaemon(true);
        udpThread.start();

        // Inicia servidor HTTP
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        System.out.println("[Gateway] Servidor HTTP iniciado na porta " + httpPort);

        server.createContext("/set", new SetProxyHandler());
        server.createContext("/get", new GetProxyHandler());
        server.createContext("/status", new StatusHandler());

        server.setExecutor(null);
        server.start();
    }

    // Handler para /set
    static class SetProxyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // 1) Descobre o líder ativo
            ServiceRegistry.NodeInfo leader = ServiceRegistry.getLeaderAtivo();
            if (leader == null) {
                send(exchange, 503,
                        "Nenhum nó LEADER ativo encontrado. " +
                        "Verifique se o nó está rodando e enviando heartbeat.");
                return;
            }

            System.out.println("[Gateway] Encaminhando SET para líder "
                    + leader.id + " (" + leader.baseUrl() + ")");

            String query = exchange.getRequestURI().getRawQuery();
            String leaderUrl = leader.baseUrl() + "/set";
            if (query != null && !query.isEmpty()) {
                leaderUrl += "?" + query;
            }

            try {
                // 2) Envia SET ao líder
                HttpRequest reqLeader = HttpRequest.newBuilder()
                        .uri(URI.create(leaderUrl))
                        .GET()
                        .build();

                HttpResponse<String> leaderResp =
                        httpClient.send(reqLeader, HttpResponse.BodyHandlers.ofString());

                if (leaderResp.statusCode() >= 400) {
                    send(exchange, leaderResp.statusCode(),
                            "Falha ao gravar no líder: " + leaderResp.body());
                    return;
                }

                // 3) Replicação mínima: tentar enviar a mesma escrita para followers ativos
                List<ServiceRegistry.NodeInfo> followers = ServiceRegistry.getFollowersAtivos();
                for (ServiceRegistry.NodeInfo f : followers) {

                    System.out.println("[Gateway] Tentando replicar SET para follower "
                            + f.id + " (" + f.baseUrl() + ")");

                    try {
                        String followerUrl = f.baseUrl() + "/set";
                        if (query != null && !query.isEmpty()) {
                            followerUrl += "?" + query;
                        }

                        HttpRequest reqFollower = HttpRequest.newBuilder()
                                .uri(URI.create(followerUrl))
                                .GET()
                                .build();

                        httpClient.send(reqFollower, HttpResponse.BodyHandlers.ofString());

                        System.out.println("[Gateway] Replicação para follower "
                                + f.id + " concluída (melhor esforço).");

                    } catch (Exception e) {
                        System.out.println("[Gateway] Falha ao replicar para follower "
                                + f.id + ": " + e.getMessage());
                    }
                }

                // 4) Responde para o cliente usando a resposta do líder
                send(exchange, 200, leaderResp.body());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                send(exchange, 500, "Erro: Thread interrompida (" + e.getMessage() + ")");
            } catch (Exception e) {
                send(exchange, 502, "Erro ao encaminhar para o líder: " + e.getMessage());
            }
        }
    }

    // Handler para /get
    static class GetProxyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            ServiceRegistry.NodeInfo node = ServiceRegistry.getNodeParaGet();

            if (node == null) {
                send(exchange, 503,
                        "Nenhum nó disponível para GET. " +
                        "Verifique se há nós ativos enviando heartbeat.");
                return;
            }
System.out.println("[Gateway] Encaminhando GET para nó " + node.id +
        " (" + node.baseUrl() + ")");

            String query = exchange.getRequestURI().getRawQuery();
            String targetUrl = node.baseUrl() + "/get";
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
                send(exchange, 500, "Erro: Thread interrompida (" + e.getMessage() + ")");
            } catch (Exception e) {
                send(exchange, 502, "Erro ao encaminhar para o nó: " + e.getMessage());
            }
        }
    }
    
static class StatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {

        StringBuilder sb = new StringBuilder();
        sb.append("===== STATUS DO CLUSTER =====\n");

        for (ServiceRegistry.NodeInfo info : ServiceRegistry.getTodosOsNos()) {

            sb.append("\nNó ").append(info.id).append(":\n");
            sb.append("  Papel: ").append(info.role).append("\n");
            sb.append("  Endereço: ").append(info.baseUrl()).append("\n");
            sb.append("  Ativo: ").append(info.ativo ? "SIM" : "NÃO").append("\n");
            sb.append("  Último heartbeat: ").append(info.lastHeartbeatMillis).append("\n");
        }

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}

    private static void send(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
