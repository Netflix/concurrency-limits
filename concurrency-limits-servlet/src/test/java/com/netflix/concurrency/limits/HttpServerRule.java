package com.netflix.concurrency.limits;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.rules.ExternalResource;

public class HttpServerRule extends ExternalResource {
    private Server server;
    private final Consumer<ServletContextHandler> customizer;
    
    public HttpServerRule(Consumer<ServletContextHandler> customizer) {
        this.customizer = customizer;
    }
    
    protected void before() throws Throwable {
        this.server = new Server(0);
        
        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        customizer.accept(context);
        server.setHandler(context);
        server.start();
    }

    /**
     * Override to tear down your specific external resource.
     */
    protected void after() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public int getPort() {
        return ((ServerConnector)server.getConnectors()[0]).getLocalPort();
    }
    
    public String get(String path) throws Exception {
        URL url = new URL("http://localhost:" + getPort() + path);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        if (responseCode != 200) {
            throw new Exception(readString(con.getInputStream()));
        } else {
            return readString(con.getInputStream());
        }
    }
    
    public String readString(InputStream is) throws IOException {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }
}
