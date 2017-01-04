package jp.piax.ofm.pubsub.web;

import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import jp.piax.ofm.pubsub.common.CommonValues;

/**
 * ダミーの認証エンジン
 * 
 * UserId == Password の場合に認証する
 */
public class DummyAuthenticator implements Authenticator {
    public DummyAuthenticator(Properties prop) {
    }

    @Override
    public String authenticate(HttpServletRequest req) {
        String[] uid = req.getParameterValues(CommonValues.LOGIN_PARAM_USERID);
        String[] pass = req.getParameterValues(CommonValues.LOGIN_PARAM_PASSWD);
        if (uid == null || uid.length < 1)
            return null;
        if (pass == null || pass.length < 1)
            return null;

        return (uid[0].equals(pass[0]) ? uid[0] : null);
    }
}
