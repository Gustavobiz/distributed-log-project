package com.dist.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registro de nós (Leader / Followers) no Gateway.
 * Controla:
 *  - registro inicial (REGISTER)
 *  - heartbeat
 *  - nós ativos / inativos
 *  - escolha de nó para GET (round-robin)
 */
public class ServiceRegistry {

    private static final Map<String, NodeInfo> registry = new ConcurrentHashMap<>();

    // Tempo máximo sem heartbeat antes de considerar o nó morto (ms)
    private static final long HEARTBEAT_TIMEOUT_MS = 5000;

    // Índice para round-robin de GET
    private static final AtomicInteger rrIndex = new AtomicInteger(0);

    public static void registerNode(String id, String ip, int port, String role) {
        NodeInfo info = new NodeInfo(id, ip, port, role);
        registry.put(id, info);

        System.out.println("[Gateway] Registro recebido: nó " + id +
                " (" + ip + ":" + port + "), papel=" + role);
    }

    public static void updateHeartbeat(String id) {
        NodeInfo info = registry.get(id);
        if (info == null) {
            System.out.println("[Gateway] Heartbeat de nó desconhecido: " + id);
            return;
        }

        info.lastHeartbeatMillis = System.currentTimeMillis();
        if (!info.ativo) {
            System.out.println("[Gateway] Nó " + id + " voltou a ficar ATIVO");
        }
        info.ativo = true;
    }

    // Retorna um líder que esteja ativo (com heartbeat recente)
    public static NodeInfo getLeaderAtivo() {
        return registry.values()
                .stream()
                .filter(info -> "LEADER".equalsIgnoreCase(info.role))
                .filter(ServiceRegistry::isAlive)
                .findFirst()
                .orElse(null);
    }

    // Followers ativos (para replicação mínima do SET)
    public static List<NodeInfo> getFollowersAtivos() {
        List<NodeInfo> followers = new ArrayList<>();
        for (NodeInfo info : registry.values()) {
            if (!"LEADER".equalsIgnoreCase(info.role) && isAlive(info)) {
                followers.add(info);
            }
        }
        return followers;
    }

    // Lista de nós ativos para GET (líder + followers)
    public static List<NodeInfo> getNosAtivosParaGet() {
        List<NodeInfo> ativos = new ArrayList<>();
        for (NodeInfo info : registry.values()) {
            if (isAlive(info)) {
                ativos.add(info);
            }
        }
        return ativos;
    }

    // Escolhe nó para GET em round-robin entre todos os ativos
    public static NodeInfo getNodeParaGet() {
        List<NodeInfo> ativos = getNosAtivosParaGet();
        if (ativos.isEmpty()) {
            return null;
        }
        int idx = Math.floorMod(rrIndex.getAndIncrement(), ativos.size());
        return ativos.get(idx);
    }

    // Verifica se nó está "vivo" e atualiza flag ativo
    private static boolean isAlive(NodeInfo info) {
        long agora = System.currentTimeMillis();
        long delta = agora - info.lastHeartbeatMillis;

        if (delta > HEARTBEAT_TIMEOUT_MS) {
            if (info.ativo) {
                info.ativo = false;
                System.out.println("[Gateway] Nó " + info.id + " ficou INATIVO (sem heartbeat)");
            }
            return false;
        }

        info.ativo = true;
        return true;
    }

    public static class NodeInfo {
        public final String id;
        public final String ip;
        public final int port;
        public final String role;

        public volatile long lastHeartbeatMillis;
        public volatile boolean ativo;

        public NodeInfo(String id, String ip, int port, String role) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.role = role;
            this.lastHeartbeatMillis = System.currentTimeMillis();
            this.ativo = true;
        }

        public String baseUrl() {
            return "http://" + ip + ":" + port;
        }
    }
}
