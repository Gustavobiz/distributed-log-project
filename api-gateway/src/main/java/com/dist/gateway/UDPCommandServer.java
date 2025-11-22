package com.dist.gateway;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * Servidor UDP para clientes (JMeter).
 *
 * Comandos aceitos no payload:
 *   SET chave valor
 *   GET chave
 *   STATUS
 *
 * Resposta é enviada no próprio UDP de volta.
 */
public class UDPCommandServer implements Runnable {

    private final int port;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public UDPCommandServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            System.out.println("[Gateway] Servidor UDP de comandos iniciado na porta " + port);

            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();

                String resposta = processarComando(msg);

                byte[] out = resposta.getBytes(StandardCharsets.UTF_8);
                DatagramPacket resp = new DatagramPacket(
                        out, out.length,
                        packet.getAddress(),
                        packet.getPort()
                );
                socket.send(resp);
            }

        } catch (Exception e) {
            System.out.println("[Gateway] Erro no servidor UDP de comandos: " + e.getMessage());
        }
    }

    private String processarComando(String cmd) {
        try {
            String[] parts = cmd.trim().split("\\s+");
            if (parts.length == 0) return "ERRO: comando vazio";

            switch (parts[0].toUpperCase()) {
                case "SET":
                    if (parts.length < 3) return "ERRO: use SET chave valor";
                    return processarSet(parts[1], parts[2]);

                case "GET":
                    if (parts.length < 2) return "ERRO: use GET chave";
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
