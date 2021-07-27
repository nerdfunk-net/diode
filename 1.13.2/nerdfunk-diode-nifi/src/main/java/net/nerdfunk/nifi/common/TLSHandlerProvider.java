package net.nerdfunk.nifi.common;

import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.CertificateException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.ssl.SSLContextService;

public class TLSHandlerProvider {

    private final String PROTOCOL = "TLS";
    private final String algorithm;
    private final String keystore;
    private final String keystore_type;
    private final String keystore_password;
    private final String truststore;
    private final String truststore_type;
    private final String truststore_password;
    private final boolean isKeystoreConfigured;
    private final boolean isTruststoreConfigured;

    private final ComponentLog logger;
    private SSLContext serverContext = null;
    private SSLContext clientContext = null;
    private TrustManagerFactory trustManagerFactory;
    private KeyManagerFactory keyManagerFactory;

    public TLSHandlerProvider(ComponentLog logger, SSLContextService sslContextService) {
        this.logger = logger;
        this.keystore = sslContextService.getKeyStoreFile();
        this.keystore_type = sslContextService.getKeyStoreType();
        this.keystore_password = sslContextService.getKeyStorePassword();
        this.truststore = sslContextService.getTrustStoreFile();
        this.truststore_type = sslContextService.getTrustStoreType();
        this.truststore_password = sslContextService.getTrustStorePassword();
        this.algorithm = sslContextService.getSslAlgorithm();
        this.isKeystoreConfigured = sslContextService.isKeyStoreConfigured();
        this.isTruststoreConfigured = sslContextService.isTrustStoreConfigured();
        this.trustManagerFactory = null;
        this.keyManagerFactory = null;
    }

    public SslHandler getServerHandler() {
        SSLEngine sslEngine = null;
        if (this.serverContext == null) {
            logger.error("Server SSL context is null");
        } else {
            sslEngine = serverContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
        }
        return new SslHandler(sslEngine);
    }

    public SslHandler getClientHandler(boolean clientauth) {
        SSLEngine sslEngine = null;
        if (this.clientContext == null) {
            logger.error("Client SSL context is null");
        } else {
            sslEngine = serverContext.createSSLEngine();
            sslEngine.setUseClientMode(true);
        }
        return new SslHandler(sslEngine);
    }
    
    public void initSSLContext() {

        try {
            String ciphers = Security.getProperty(algorithm);
            if (ciphers == null) {
                ciphers = TrustManagerFactory.getDefaultAlgorithm();
            }

            // keystore
            KeyManager[] keyManagers = null;
            if (isKeystoreConfigured) {
                KeyStore ks = KeyStore.getInstance(keystore_type);
                ks.load(Files.newInputStream(Paths.get(keystore)), keystore_password.toCharArray());

                // set up key manager factory
                keyManagerFactory = KeyManagerFactory.getInstance(ciphers);
                keyManagerFactory.init(ks,keystore_password.toCharArray());
                keyManagers = keyManagerFactory.getKeyManagers();
            }
            
            // truststore
            TrustManager[] trustManagers = null;
            if (isTruststoreConfigured) {
                KeyStore ts = KeyStore.getInstance(truststore_type);
                ts.load(Files.newInputStream(Paths.get(truststore)), truststore_password.toCharArray());

                // set up trust manager factory
                trustManagerFactory = TrustManagerFactory.getInstance(ciphers);
                trustManagerFactory.init(ts);
                trustManagers = trustManagerFactory.getTrustManagers();
            }

            serverContext = SSLContext.getInstance(PROTOCOL);
            serverContext.init(keyManagers, trustManagers, null);
            
            clientContext = SSLContext.getInstance(PROTOCOL);
            clientContext.init(keyManagers, trustManagers, null);
        } catch (IOException | KeyManagementException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException | CertificateException e) {
            throw new Error("Failed to initialize the SSLContext", e);
        }
    }
}
