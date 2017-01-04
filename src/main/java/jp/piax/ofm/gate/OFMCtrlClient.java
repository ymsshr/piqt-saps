package jp.piax.ofm.gate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Set;

import jp.piax.ofm.gate.common.OFMCtrlAPIException;
import jp.piax.ofm.gate.messages.IPMACPair;

import org.apache.http.Consts;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OFM 設定 WebAPI へのアクセサ
 */
public class OFMCtrlClient {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(OFMCtrlClient.class);

    private static final String CONTENT_TYPE = "application/json";
    private static final int HTTP_CON_TIMEOUT = 1000;   // OFM WebAPI 接続タイムアウト (ms)
    private static final int HTTP_REQ_TIMEOUT = 5000;   // OFM WebAPI 要求処理タイムアウト (ms)

    protected URI ctrlEndpoint; // OFM 設定 WebAPI Endpoint
    protected int httpConnectTimeout = HTTP_CON_TIMEOUT;   // OFM WebAPI 接続タイムアウト (ms)
    protected int httpRequestTimeout = HTTP_REQ_TIMEOUT;   // OFM WebAPI 要求処理タイムアウト (ms)

    /**
     * コンストラクタ
     * @param endpoint OFM 設定 WebAPI Endpoint
     * @param contimeout OFM サーバへの接続タイムアウト (ms)
     * @param reqtimeout OFM 設定 Web APIの要求処理タイムアウト (ms)
     */
    public OFMCtrlClient(URI endpoint, int contimeout, int reqtimeout) {
        if (endpoint == null)
            throw new NullPointerException("endpoint should not be null");
        if (!endpoint.isAbsolute())
            throw new IllegalArgumentException("endpoint should be absolute");
        if (contimeout < 0)
            throw new IllegalArgumentException("contimeout should be positive");
        if (reqtimeout < 0)
            throw new IllegalArgumentException("reqtimeout should be positive");

        if (!endpoint.toString().endsWith("/")) {
            endpoint = URI.create(endpoint.toString()+"/");
        }
        this.ctrlEndpoint = endpoint;
        this.httpConnectTimeout = contimeout;
        this.httpRequestTimeout = reqtimeout;
    }


    /**
     * Apache HttpClient を取得する
     * @return
     */
    private CloseableHttpClient getHttpClient() {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(httpConnectTimeout)
                .setConnectionRequestTimeout(httpRequestTimeout)
                .setSocketTimeout(httpRequestTimeout).build();
        return  HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    /**
     * setupOFM リクエストを出す
     * @param subscribers subscriber の OFM 受信アドレス
     * @return 成功時は OFM アドレス 失敗時は null
     * @throws JSONException 
     * @throws OFMCtrlAPIException API の呼び出しにエラーが生じた場合
     * @throws IOException
     */
    public InetSocketAddress setupOFM(Set<IPMACPair> subscribers) throws JSONException, OFMCtrlAPIException, IOException {
        if (subscribers == null)
            throw new NullPointerException("subscribers should not be null");
        if (subscribers.isEmpty())
            throw new IllegalArgumentException("subscribers should have one more elements");

        // build up API URL
        String url = this.ctrlEndpoint.toString() + "setupOFM";
        // build request JSON
        JSONObject req = new JSONObject();
        JSONArray addrs = new JSONArray();
        for (IPMACPair addr : subscribers) {
            String addrstr = IPMACPair.convertToOFMAddrFormat(addr);
            addrs.put(addrstr);
        }
        req.put("memberAddrs", addrs);

        if (logger.isTraceEnabled()) {
            logger.trace("setupOFM Method:POST, URL:[{}],  Body:[{}]", url, req.toString());
        }

        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-Type", CONTENT_TYPE);
            request.setEntity(new StringEntity(req.toString(), Consts.UTF_8));

            // send request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // response check
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() != 200 && status.getStatusCode() != 204 && status.getStatusCode() != 205) {
                    logger.error("Bad HTTP status code : {}", status.getStatusCode());
                    throw new OFMCtrlAPIException("Bad HTTP status code : " + status.getStatusCode());
                }

                String result = EntityUtils.toString(response.getEntity());
                if (logger.isTraceEnabled()) {
                    logger.trace("setupOFM result Status:[{}], Body:[{}]", status.getStatusCode(), result);
                }

                // parse result
                JSONObject resp = new JSONObject(result);
                if (resp.has("result")) {
                    String[] ofmaddr_str = resp.getString("result").split(":");
                    if (ofmaddr_str.length != 2) {
                        logger.error("Bad OFM address format : {}", result);
                        throw new OFMCtrlAPIException("Bad OFM address format : " + result);
                    }

                    InetSocketAddress ret = new InetSocketAddress(ofmaddr_str[0],Integer.parseInt(ofmaddr_str[1]));
                    if (ret.isUnresolved()) {
                        throw new OFMCtrlAPIException("Bad result : " + result);
                    }
                    return ret;
                } else if (resp.has("error")) {
                    JSONObject error = resp.getJSONObject("error");
                    String errormsg = String.format("API Error : (%s) %s", error.getString("code"), error.getString("message"));
                    logger.error(errormsg);
                    throw new OFMCtrlAPIException(errormsg);
                } else {
                    logger.error("Invalid result format : {}" + result);
                    throw new OFMCtrlAPIException("Invalid result format : " + result);
                }
            }
        }
    }

    /**
     * clearOFM リクエストを出す(GET使用)
     * @param ofmaddress clearOFM の対象となる OFM アドレス
     * @throws OFMCtrlAPIException API の呼び出しにエラーが生じた場合
     * @throws IOException
     */
    public void clearOFM(InetSocketAddress ofmaddress) throws OFMCtrlAPIException, IOException {
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");
        if (ofmaddress.getAddress() == null)
            throw new NullPointerException("ofmaddress requires IP address");

        // build up API URL
        String url = this.ctrlEndpoint.toString() + "clearOFM/" + ofmaddress.getAddress().getHostAddress() + ":" + ofmaddress.getPort();

        logger.trace("clearOFM Method:POST, URL:[{}]", url);

        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpGet request = new HttpGet(url);

            // send request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // response check
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() != 200 && status.getStatusCode() != 204 && status.getStatusCode() != 205) {
                    logger.error("Bad HTTP status code : {}", status.getStatusCode());
                    throw new OFMCtrlAPIException("Bad HTTP status code : " + status.getStatusCode());
                }

                String result = EntityUtils.toString(response.getEntity());
                if (logger.isTraceEnabled()) {
                    logger.trace("clearOFM result Status:[{}], Body:[{}]", status.getStatusCode(), result);
                }
            }
        }
    }

    /**
     * clearOFM リクエストを出す(DELETE使用)
     * @param ofmaddress clearOFM の対象となる OFM アドレス
     * @throws OFMCtrlAPIException API の呼び出しにエラーが生じた場合
     * @throws IOException
     */
    public void clearOFMREST(InetSocketAddress ofmaddress) throws OFMCtrlAPIException, IOException {
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");
        if (ofmaddress.getAddress() == null)
            throw new NullPointerException("ofmaddress requires IP address");

        // build up API URL
        String url = this.ctrlEndpoint.toString() + ofmaddress.getAddress().getHostAddress() + ":" + ofmaddress.getPort();

        logger.trace("clearOFM(REST) Method:DELETE, URL:[{}]", url);

        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpDelete request = new HttpDelete(url);

            // send request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // response check
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() != 200 && status.getStatusCode() != 204 && status.getStatusCode() != 205) {
                    logger.error("Bad HTTP status code : {}", status.getStatusCode());
                    throw new OFMCtrlAPIException("Bad HTTP status code : " + status.getStatusCode());
                }

                String result = EntityUtils.toString(response.getEntity());
                if (logger.isTraceEnabled()) {
                    logger.trace("clearOFM(REST) rresult Status:[{}], Body:[{}]", status.getStatusCode(), result);
                }
            }
        }
    }

    /**
     * addOFMMember リクエストを出す(POST使用)
     * @param ofmaddress 追加対象の OFM アドレス
     * @param subscribers 追加する OFM 受信アドレス
     * @throws JSONException 
     * @throws OFMCtrlAPIException API の呼び出しにエラーが生じた場合
     * @throws IOException 
     */
    public void addOFMMembers(InetSocketAddress ofmaddress, Set<IPMACPair> subscribers) throws JSONException, OFMCtrlAPIException, IOException {
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");
        if (ofmaddress.getAddress() == null)
            throw new NullPointerException("ofmaddress requires IP address");
        if (subscribers == null)
            throw new NullPointerException("subscribers should not be null");
        if (subscribers.isEmpty())
            throw new IllegalArgumentException("subscribers should have one more elements");

        // build up API URL
        String url = this.ctrlEndpoint.toString() + "addOFMMembers/" + ofmaddress.getAddress().getHostAddress() + ":" + ofmaddress.getPort();
        // build request JSON
        JSONObject req = new JSONObject();
        JSONArray addrs = new JSONArray();
        for (IPMACPair addr : subscribers) {
            String addrstr = IPMACPair.convertToOFMAddrFormat(addr);
            addrs.put(addrstr);
        }
        req.put("memberAddrs", addrs);

        if (logger.isTraceEnabled()) {
            logger.trace("addOFMMembers Method:POST, URL:[{}], Body:[{}]", url, req.toString());
        }

        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-Type", CONTENT_TYPE);
            request.setEntity(new StringEntity(req.toString(), Consts.UTF_8));

            // send request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // response check
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() != 200 && status.getStatusCode() != 204 && status.getStatusCode() != 205) {
                    logger.error("Bad HTTP status code : {}", status.getStatusCode());
                    throw new OFMCtrlAPIException("Bad HTTP status code : " + status.getStatusCode());
                }

                String result = EntityUtils.toString(response.getEntity());
                if (logger.isTraceEnabled()) {
                    logger.trace("addOFMMembers result Status:[{}], Body:[{}]", status.getStatusCode(), result);
                }
            }
        }
    }

    /**
     * removeOFMMember リクエストを出す(POST使用)
     * @param ofmaddress 削除対象の OFM アドレス
     * @param subscribers 追加する OFM 受信アドレス
     * @throws JSONException 
     * @throws OFMCtrlAPIException API の呼び出しにエラーが生じた場合
     * @throws IOException 
     */
    public void removeOFMMembers(InetSocketAddress ofmaddress, Set<IPMACPair> subscribers) throws IOException, JSONException {
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");
        if (ofmaddress.getAddress() == null)
            throw new NullPointerException("ofmaddress requires IP address");
        if (subscribers == null)
            throw new NullPointerException("subscribers should not be null");
        if (subscribers.isEmpty())
            throw new IllegalArgumentException("subscribers should have one more elements");

        // build up API URL
        String url = this.ctrlEndpoint.toString() + "removeOFMMembers/" + ofmaddress.getAddress().getHostAddress() + ":" + ofmaddress.getPort();
        // build request JSON
        JSONObject req = new JSONObject();
        JSONArray addrs = new JSONArray();
        for (IPMACPair addr : subscribers) {
            String addrstr = IPMACPair.convertToOFMAddrFormat(addr);
            addrs.put(addrstr);
        }
        req.put("memberAddrs", addrs);

        if (logger.isTraceEnabled()) {
            logger.trace("removeOFMMembers Method:POST, URL:[{}], Body:[{}]", url, req.toString());
        }

        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("Content-Type", CONTENT_TYPE);
            request.setEntity(new StringEntity(req.toString(), Consts.UTF_8));

            // send request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // response check
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() != 200 && status.getStatusCode() != 204 && status.getStatusCode() != 205) {
                    logger.error("Bad HTTP status code : {}", status.getStatusCode());
                    throw new OFMCtrlAPIException("Bad HTTP status code : " + status.getStatusCode());
                }

                String result = EntityUtils.toString(response.getEntity());
                if (logger.isTraceEnabled()) {
                    logger.trace("removeOFMMembers result Status:[{}], Body:[{}]", status.getStatusCode(), result);
                }
            }
        }
    }
}
