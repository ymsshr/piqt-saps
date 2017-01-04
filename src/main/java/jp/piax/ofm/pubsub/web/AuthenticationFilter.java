package jp.piax.ofm.pubsub.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.common.CommonValues;

/**
 * Session による認証情報を要求する DefaultServlet
 * 
 * 未認証時は
 *  A) 401 unauthorized を返す
 *  B) 指定の URL に Redirect する
 * のどちらかの動作をインスタンス生成時に選択する。
 */
public class AuthenticationFilter implements Filter {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationFilter.class);
    
    private String loginRedirectURL = null;

    /**
     * 未認証時に 401 unauthorized を返す Filter を作成する
     */
    public AuthenticationFilter() {
        loginRedirectURL = null;
    }

    /**
     * 未認証時にリダイレクトする Filter を作成する
     * 
     * @param loginredirecturl 未認証時のリダイレクト先URL
     */
    public AuthenticationFilter(String loginredirecturl) {
        if (loginredirecturl == null)
            throw new NullPointerException("loginredirecturl should not be null");
        if (loginredirecturl.isEmpty())
            throw new IllegalArgumentException("loginredirecturl should not be empty");

        this.loginRedirectURL = loginredirecturl;
    }

    /* (非 Javadoc)
     * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
     */
    @Override
    public void init(FilterConfig config) throws ServletException {
        // nothing todo
    }

    /* (非 Javadoc)
     * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest, javax.servlet.ServletResponse, javax.servlet.FilterChain)
     */
    @Override
    public void doFilter(ServletRequest raw_req, ServletResponse raw_resp,
            FilterChain chain) throws IOException, ServletException {
        logger.trace("doFilter");
        HttpServletRequest req = (HttpServletRequest) raw_req;
        HttpServletResponse resp = (HttpServletResponse) raw_resp;

        HttpSession session = req.getSession(false);

        // セッションが無い場合
        if (session == null) {
            logger.warn("No session : {}", req.getRemoteAddr());
            if (loginRedirectURL != null) {
                resp.sendRedirect(loginRedirectURL);
            } else {
                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "No valid session");
            }
            chain.doFilter(req, resp);
            return;
        }

        // 認証キー（CommonValues.SESSION_USERID）が無い場合
        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        if (userid == null) {
            logger.warn("Not authenticated session : {}", req.getRemoteAddr());
            if (loginRedirectURL != null) {
                resp.sendRedirect(loginRedirectURL);
            } else {
                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Not authorized");
            }
        }
        chain.doFilter(req, resp);
    }

    /* (非 Javadoc)
     * @see javax.servlet.Filter#destroy()
     */
    @Override
    public void destroy() {
        // nothing todo
    }
}
