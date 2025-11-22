package com.dist.gateway;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

/**
 * Servidor UDP que escuta:
 *  - REGISTER;id;ip;port;role
 *  - HEARTBEAT;id
 */
public class UDPRegisterServer implements Runnable {

    private final int port;

    public UDPRegisterServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            System.out.println("[Gateway] Servidor UDP iniciado na porta " + port +
                    " (REGISTER + HEARTBEAT)");

            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                process(msg);
            }
        } catch (Exception e) {
            System.out.println("[Gateway] Erro no servidor UDP: " + e.getMessage());
        }
    }

    private void process(String msg) {
        if (msg.startsWith("REGISTER;")) {
            processRegister(msg);
        } else if (msg.startsWith("HEARTBEAT;")) {
            processHeartbeat(msg);
        } else {
            System.out.println("[Gateway] Mensagem UDP desconhecida: " + msg);
        }
    }

    private void processRegister(String msg) {
        String[] parts = msg.split(";");
        if (parts.length != 5) {
            System.out.println("[Gateway] Formato inválido de REGISTER: " + msg);
            return;
        }

        String id = parts[1];
        String ip = parts[2];
        int port = Integer.parseInt(parts[3]);
        String role = parts[4];

        ServiceRegistry.registerNode(id, ip, port, role);
    }

    private void processHeartbeat(String msg) {
        String[] parts = msg.split(";");
        if (parts.length != 2) {
            System.out.println("[Gateway] Formato inválido de HEARTBEAT: " + msg);
            return;
        }

        String id = parts[1];
        ServiceRegistry.updateHeartbeat(id);
    }
}
