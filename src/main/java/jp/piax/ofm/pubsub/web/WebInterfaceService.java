package jp.piax.ofm.pubsub.web;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.Servlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;
import jp.piax.ofm.pubsub.web.servlet.APIServlet;
import jp.piax.ofm.pubsub.web.servlet.AuthenticatedDefaultServlet;
import jp.piax.ofm.pubsub.web.servlet.LoginServlet;
import jp.piax.ofm.pubsub.web.servlet.LogoutServlet;
import jp.piax.ofm.pubsub.web.websocket.WSServlet;

/**
 * Web インタフェースを提供するサービスクラス
 */
public class WebInterfaceService {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(WebInterfaceService.class);

    private int webPort = -1;
    private String documentRoot = null;
    private String loginPage = null;
    private String userPage = null;
    private int sessionTimeout = -1;
    private Authenticator authenticator = null;
    private WebUserAdaptorManager adaptorManager = null;

    private Server server = null;
    private boolean active = false;
    private boolean started = false;

    /**
     * 
     * @param webport
     * @param documentroot
     * @param loginpage
     * @param userpage
     * @param sessiontimeout
     * @param authenticator
     * @param adaptor_manager
     */
    public WebInterfaceService(int webport, String documentroot, String loginpage, String userpage,
            int sessiontimeout, Authenticator authenticator, WebUserAdaptorManager adaptor_manager) {
        if (webport <= 0 || 65535 < webport)
            throw new IllegalArgumentException("webport should be between 1 to 65535");
        if (documentroot == null)
            throw new NullPointerException("documentroot should not be null");
        if (documentroot.isEmpty())
            throw new IllegalArgumentException("documentroot should not be empty");
        if (loginpage == null)
            throw new NullPointerException("loginpage should not be null");
        if (loginpage.isEmpty())
            throw new IllegalArgumentException("loginpage should not be empty");
        if (userpage == null)
            throw new NullPointerException("userpage should not be null");
        if (userpage.isEmpty())
            throw new IllegalArgumentException("userpage should not be empty");
        if (authenticator == null)
            throw new NullPointerException("authenticator should not be null");
        if (adaptor_manager == null)
            throw new NullPointerException("adaptor_manager should not be null");

        this.webPort = webport;
        this.documentRoot = documentroot;
        this.loginPage = loginpage;
        this.userPage = userpage;
        this.sessionTimeout = sessiontimeout;
        this.authenticator = authenticator;
        this.adaptorManager = adaptor_manager;
    }

    public boolean isActive() {
        return active;
    }

    public synchronized void start() throws Exception {
        if (isActive())
            throw new IllegalStateException("This instance is already started");
        if (started)
            throw new IllegalStateException("Used instance");

        // init Jetty
        server = new Server(webPort);
        server.setStopAtShutdown(true);
        ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);

        // set default web servlet and document root.
        root.setResourceBase(documentRoot);
        root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

        // setup servlets
        Servlet servlet = new AuthenticatedDefaultServlet(loginPage);

        root.addServlet(new ServletHolder(servlet), "/*");      // デフォルトは認証必要
        root.addServlet(DefaultServlet.class, "/login.html");   // ログインページとリソース類は除外
        root.addServlet(DefaultServlet.class, "/css/*");
        root.addServlet(DefaultServlet.class, "/js/*");
        root.addServlet(DefaultServlet.class, "/image/*");

        LoginServlet.SessionTimeout = sessionTimeout;       // HTTP session の有効期間指定
        root.addServlet(new ServletHolder(new LoginServlet(loginPage, userPage, authenticator)), "/login");
        root.addServlet(new ServletHolder(new LogoutServlet(loginPage)), "/logout");
        root.addServlet(new ServletHolder(new WSServlet(adaptorManager)), "/ws");
        root.addServlet(new ServletHolder(new APIServlet(adaptorManager)), "/api");

        // 認証フィルタの登録
        Filter authfilter = new AuthenticationFilter();
        root.addFilter(new FilterHolder(authfilter), "/ws", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(new FilterHolder(authfilter), "/api", EnumSet.of(DispatcherType.REQUEST));

        // HTTP Session の invalidate 検出用 listener 登録
        root.getSessionHandler().addEventListener(new SessionListener(adaptorManager));

        // Jettyサーバ起動
        logger.info("Start Jetty");
        server.start();

        active = true;
        started = true;
    }

    public synchronized void stop() throws Exception {
        if (server != null) {
            // Jettyサーバ停止
            logger.info("Stop Jetty");
            server.stop();
            server.join();
        }
        active = false;
    }
}
