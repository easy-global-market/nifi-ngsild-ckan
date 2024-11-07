package egm.io.nifi.processors.ckan.http;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class HttpBackend {
    private final LinkedList<String> hosts;
    private final String port;
    private final boolean ssl;
    private final HttpClient httpClient;
    private static final Logger logger = LoggerFactory.getLogger(HttpBackend.class);

    public HttpBackend(String[] hosts, String port, boolean ssl) {
        this.hosts = new LinkedList<>(Arrays.asList(hosts));
        this.port = port;
        this.ssl = ssl;

        // create a Http clients factory and an initial connection
        HttpClientFactory httpClientFactory = new HttpClientFactory(ssl);
        httpClient = httpClientFactory.getHttpClient(ssl);
    } // HttpBackend

    public JsonResponse doRequest(String method, String url, boolean relative, ArrayList<Header> headers,
                                  StringEntity entity) throws Exception {
        JsonResponse response;

        if (relative) {
            // iterate on the hosts
            for (String host : hosts) {
                // create the HttpFS URL
                String effectiveURL = (ssl ? "https://" : "http://") + host + ":" + port + url;

                try {
                    response = doRequest(method, effectiveURL, headers, entity);
                } catch (Exception e) {
                    logger.error("There was a problem when performing the request (details=\"{}\"). "
                            + "Most probably the used http endpoint (\"{}\") is not active, trying another one", e.getMessage(), host);
                    continue;
                } // try catch

                // place the current host in the first place (if not yet placed), since it is currently working
                if (!hosts.getFirst().equals(host)) {
                    hosts.remove(host);
                    hosts.add(0, host);
                    logger.info("Placing the host in the first place of the list (host=\"{}\")", host);
                } // if

                return response;
            } // for

            throw new Exception("No http endpoint was reachable");
        } else {
            return doRequest(method, url, headers, entity);
        } // if else
    } // doRequest

    protected JsonResponse doRequest(String method, String url, ArrayList<Header> headers, StringEntity entity)
            throws Exception {
        HttpResponse httpRes;
        HttpRequestBase request;

        if ("PUT".equals(method)) {
            HttpPut req = new HttpPut(url);

            if (entity != null) {
                req.setEntity(entity);
            } // if

            request = req;
        } else if ("POST".equals(method)) {
            HttpPost req = new HttpPost(url);

            if (entity != null) {
                req.setEntity(entity);
            } // if

            request = req;
        } else if ("GET".equals(method)) {
            request = new HttpGet(url);
        } else if ("DELETE".equals(method)) {
            request = new HttpDelete(url);
        } else {
            throw new Exception("HTTP method not supported: " + method);
        } // if else

        if (headers != null) {
            for (Header header : headers) {
                request.setHeader(header);
            } // for
        } // if

        logger.debug("Http request: {}", request);

        try {
            httpRes = httpClient.execute(request);
        } catch (IOException e) {
            request.releaseConnection();
            throw new Exception(e.getMessage());
        } // try catch

        JsonResponse response = createJsonResponse(httpRes);
        request.releaseConnection();
        return response;
    } // doRequest

    private JsonResponse createJsonResponse(HttpResponse httpRes) throws Exception {
        try {
            if (httpRes == null) {
                return null;
            } // if

            logger.info("Http response status line: {}", httpRes.getStatusLine().toString());

            // parse the httpRes payload
            JSONObject jsonPayload = null;
            HttpEntity entity = httpRes.getEntity();

            if (entity != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(httpRes.getEntity().getContent()));
                String res = "";
                String line;

                while ((line = reader.readLine()) != null) {
                    res += line;
                } // while

                reader.close();
                logger.debug("Http response payload: {}", res);

                if (!res.isEmpty()) {
                    JSONParser jsonParser = new JSONParser();

                    if (res.startsWith("[")) {
                        Object object = jsonParser.parse(res);
                        jsonPayload = new JSONObject();
                        jsonPayload.put("result", object);
                    } else {
                        jsonPayload = (JSONObject) jsonParser.parse(res);
                    } // if else
                } // if
            } // if

            // get the location header
            Header locationHeader = null;
            Header[] headers = httpRes.getHeaders("Location");

            if (headers.length > 0) {
                locationHeader = headers[0];
            } // if

            // return the result
            return new JsonResponse(jsonPayload, httpRes.getStatusLine().getStatusCode(),
                    httpRes.getStatusLine().getReasonPhrase(), locationHeader);
        } catch (IOException | IllegalStateException e) {
            throw new Exception(e.getMessage());
        } // try catch
    } // createJsonResponse
}
