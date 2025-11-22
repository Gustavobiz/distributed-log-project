package com.dist.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registro de nós (Leader / Followers) no Gateway.
 * Agora:
 *  - mantém apenas 1 líder por vez (currentLeaderId)
 *  - faz eleição automática
 *  - demove líderes antigos para FOLLOWER quando necessário
 */
public class ServiceRegistry {

    private static final Map<String, NodeInfo> registry = new ConcurrentHashMap<>();

    // Tempo máximo sem heartbeat antes de considerar o nó morto (ms)
    private static final long HEARTBEAT_TIMEOUT_MS = 5000;

    // Índice para round-robin de GET
    private static final AtomicInteger rrIndex = new AtomicInteger(0);

    // ID do líder atual decidido pelo Gateway
    private static volatile String currentLeaderId = null;

    public static void registerNode(String id, String ip, int port, String roleHint) {
        NodeInfo info = new NodeInfo(id, ip, port, roleHint);
        registry.put(id, info);

        // Decisão do papel real é do Gateway
        if (currentLeaderId == null) {
            currentLeaderId = id;
            info.role = "LEADER";
            System.out.println("[Gateway] Nó " + id + " definido como LÍDER atual.");
        } else {
            info.role = "FOLLOWER";
            System.out.println("[Gateway] Nó " + id + " registrado como FOLLOWER. Líder atual: " + currentLeaderId);
        }

        System.out.println("[Gateway] Registro recebido: nó " + id +
                " (" + ip + ":" + port + "), papel recebido=" + roleHint +
                ", papel efetivo=" + info.role);
    }

    public static void updateHeartbeat(String id) {
        NodeInfo info = registry.get(id);
        if (info == null) {
            System.out.println("[Gateway] Heartbeat de nó desconhecido: " + id);
            return;
        }

        info.lastHeartbeatMillis = System.currentTimeMillis();
        // Aqui não mexemos em papel, só marcamos vivo (o monitor cuida do resto)
    }

    /** Chamada pelo monitor: atualiza ativo/inativo e garante que haja 1 líder ativo. */
    public static void verificarTodosOsNos() {
        for (NodeInfo info : registry.values()) {
            isAlive(info); // atualiza flag ativo e imprime logs de transição
        }
        garantirLeaderAtivo();
    }

    /** Garante que existe um líder ativo; se não tiver, elege um follower. */
    private static synchronized void garantirLeaderAtivo() {
        // Se temos um líder atual, e ele existe e está ativo, beleza
        if (currentLeaderId != null) {
            NodeInfo leader = registry.get(currentLeaderId);
            if (leader != null && leader.ativo) {
                leader.role = "LEADER";
                return;
            }
        }

        // Se chegou aqui, não há líder ativo → tentar eleger
        NodeInfo novo = promoverFollowerParaLeader();
        if (novo == null) {
            currentLeaderId = null;
        } else {
            currentLeaderId = novo.id;
        }
    }

    /** Usa para /set – sempre devolve o líder ativo, elegendo se necessário. */
    public static NodeInfo getLeaderAtivo() {
        garantirLeaderAtivo();
        if (currentLeaderId == null) return null;
        NodeInfo leader = registry.get(currentLeaderId);
        if (leader != null && leader.ativo) return leader;
        return null;
    }

    /** Followers ativos (para replicação mínima do SET). */
    public static List<NodeInfo> getFollowersAtivos() {
        List<NodeInfo> followers = new ArrayList<>();
        for (NodeInfo info : registry.values()) {
            if (!"LEADER".equalsIgnoreCase(info.role) && info.ativo) {
                followers.add(info);
            }
        }
        return followers;
    }

    /** Lista de nós ativos para GET (líder + followers). */
    public static List<NodeInfo> getNosAtivosParaGet() {
        List<NodeInfo> ativos = new ArrayList<>();
        for (NodeInfo info : registry.values()) {
            if (info.ativo) {
                ativos.add(info);
            }
        }
        return ativos;
    }

    /** Escolhe nó para GET em round-robin entre todos os ativos. */
    public static NodeInfo getNodeParaGet() {
        List<NodeInfo> ativos = getNosAtivosParaGet();
        if (ativos.isEmpty()) {
            return null;
        }
        int idx = Math.floorMod(rrIndex.getAndIncrement(), ativos.size());
        return ativos.get(idx);
    }

    /** Elege um novo líder a partir dos nós ativos. */
    public static NodeInfo promoverFollowerParaLeader() {
        List<NodeInfo> ativos = getNosAtivosParaGet();
        if (ativos.isEmpty()) {
            System.out.println("[Gateway] Nenhum nó disponível para ser LÍDER.");
            return null;
        }

        // Estratégia simples: pega o primeiro nós ativos (poderia ordenar por id)
        NodeInfo novoLeader = ativos.get(0);

        // Ajusta papéis
        for (NodeInfo n : registry.values()) {
            if (n.id.equals(novoLeader.id)) {
                n.role = "LEADER";
            } else {
                n.role = "FOLLOWER";
            }
        }

        System.out.println("[Gateway] Eleição concluída! Novo líder eleito: " + novoLeader.id +
                " (" + novoLeader.baseUrl() + ")");
        return novoLeader;
    }

    /** Método usado pelo /status. */
    public static List<NodeInfo> getTodosOsNos() {
        return new ArrayList<>(registry.values());
    }

    // Verifica se nó está "vivo" com base no tempo do último heartbeat
    private static boolean isAlive(NodeInfo info) {
        long agora = System.currentTimeMillis();
        long delta = agora - info.lastHeartbeatMillis;

        boolean estavaAtivo = info.ativo;
        boolean estaAtivo = delta <= HEARTBEAT_TIMEOUT_MS;

        info.ativo = estaAtivo;

        if (estavaAtivo && !estaAtivo) {
            System.out.println("[Gateway] Nó " + info.id + " ficou INATIVO (sem heartbeat)");
        } else if (!estavaAtivo && estaAtivo) {
            System.out.println("[Gateway] Nó " + info.id + " voltou a ficar ATIVO");
        }

        return estaAtivo;
    }

    public static class NodeInfo {
        public final String id;
        public final String ip;
        public final int port;
        public String role; // agora pode ser alterado (LEADER/FOLLOWER)

        public volatile long lastHeartbeatMillis;
        public volatile boolean ativo;

        public NodeInfo(String id, String ip, int port, String roleHint) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.role = roleHint; // valor inicial, será ajustado pelo Gateway
            this.lastHeartbeatMillis = System.currentTimeMillis();
            this.ativo = true;
        }

        public String baseUrl() {
            return "http://" + ip + ":" + port;
        }
    }
}
