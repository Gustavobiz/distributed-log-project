package com.dist.gateway;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;

/**
 * Servidor TCP simples para o JMeter.
 * Interpreta comandos SET key value
 *                               GET key
 *                               STATUS
 */
public class TCPServer implements Runnable {

    private final int port;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public TCPServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("[Gateway] Servidor TCP iniciado na porta " + port);

            while (true) {
                Socket socket = server.accept();
                new Thread(() -> handle(socket)).start();
            }

        } catch (Exception e) {
            System.out.println("[Gateway] Erro no servidor TCP: " + e.getMessage());
        }
    }

private void handle(Socket socket) {
    try (
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer =
                    new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))
    ) {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue; // ignora linhas em branco
            }

            // Se quiser um comando de saída:
            if (line.trim().equalsIgnoreCase("QUIT")) {
                writer.write("Encerrando conexão.\n");
                writer.flush();
                break;
            }

            String resposta = processarComando(line);
            writer.write(resposta + "\n");
            writer.flush();
        }

    } catch (Exception e) {
        System.out.println("[Gateway] Erro ao processar TCP: " + e.getMessage());
    }
}


    private String processarComando(String cmd) {
        try {
            String[] parts = cmd.trim().split("\\s+");

            if (parts.length == 0) return "ERRO: comando vazio";

            switch (parts[0].toUpperCase()) {

                case "SET":
                    if (parts.length < 3)
                        return "ERRO: use SET chave valor";
                    return processarSet(parts[1], parts[2]);

                case "GET":
                    if (parts.length < 2)
                        return "ERRO: use GET chave";
                    return processarGet(parts[1]);

                case "STATUS":
                    return gerarStatus();

                default:
                    return "ERRO: comando desconhecido";
            }
        } catch (Exception e) {
            return "ERRO ao processar comando: " + e.getMessage();
        }
    }

    private String processarSet(String key, String value) {
        ServiceRegistry.NodeInfo leader = ServiceRegistry.getLeaderAtivo();
        if (leader == null) return "ERRO: nenhum líder ativo";

        String url = leader.baseUrl() + "/set?key=" + key + "&value=" + value;

        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> resp =
                    httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            return resp.body();
        } catch (Exception e) {
            return "ERRO SET: " + e.getMessage();
        }
    }

    private String processarGet(String key) {
        ServiceRegistry.NodeInfo node = ServiceRegistry.getNodeParaGet();
        if (node == null) return "ERRO: nenhum nó ativo";

        String url = node.baseUrl() + "/get?key=" + key;

        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> resp =
                    httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            return resp.body();
        } catch (Exception e) {
            return "ERRO GET: " + e.getMessage();
        }
    }

    private String gerarStatus() {
        StringBuilder sb = new StringBuilder();
        for (ServiceRegistry.NodeInfo info : ServiceRegistry.getTodosOsNos()) {
            sb.append(info.id)
              .append(" | ")
              .append(info.role)
              .append(" | ativo=")
              .append(info.ativo)
              .append("\n");
        }
        return sb.toString();
    }
}
