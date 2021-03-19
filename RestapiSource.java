package com.fs.source;

import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *   StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *   ParameterTool parameterTool = ParameterTool.fromArgs(args);

 *   String header = "{'app-token': '$2a$10$W/mKbzvGuYZ'}";
 *   DataStreamSource<String> source = env.addSource(new RestapiSource(url, "GET",header,null,1000L));
 *   source.map(s -> s).print();
 *   env.execute();
 */
public class RestapiSource extends RichParallelSourceFunction<String> {

    protected String url;
    protected String method;
    protected transient CloseableHttpClient httpClient;
    protected String header;
    protected String requestBody;
    protected long delay;
    private boolean running;

    public RestapiSource(String url, String method, String header, String requestBody, long delay) {
        this.url = url;
        this.method = method;
        this.header = header;
        this.requestBody = requestBody;
        this.delay = delay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        running = true;
        httpClient = HttpUtil.getHttpClient();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        HttpUriRequest request = HttpUtil.getRequest(method, requestBody, header, url);
        while (running) {
            try {
                CloseableHttpResponse httpResponse = httpClient.execute(request);
                HttpEntity entity = httpResponse.getEntity();
                if (entity != null) {
                    String entityData = EntityUtils.toString(entity);
                    sourceContext.collect(entityData);
                } else {
                    throw new RuntimeException("entity is null");
                }

            } catch (Exception e) {
                throw new RuntimeException("get entity error");
            }
            Thread.sleep(delay);
        }
    }

    @Override
    public void close() throws Exception {
        HttpUtil.closeClient(httpClient);
        super.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}

enum HttpMethod {
    // http 请求方式
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    COPY,
    HEAD,
    OPTIONS,
    LINK,
    UNLINK,
    PURGE,
    LOCK,
    UNLOCK,
    PROPFIND,
    VIEW
}

class HttpUtil {
    protected static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);
    private static final int COUNT = 32;
    private static final int TOTAL_COUNT = 1000;
    private static final int TIME_OUT = 5000;
    private static final int EXECUTION_COUNT = 5;

    public static Gson gson = new Gson();

    public static CloseableHttpClient getHttpClient() {
        // 设置自定义的重试策略
        MyServiceUnavailableRetryStrategy strategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(EXECUTION_COUNT)
                .retryInterval(1000)
                .build();
        // 设置自定义的重试Handler
        MyHttpRequestRetryHandler retryHandler = new MyHttpRequestRetryHandler
                .Builder()
                .executionCount(EXECUTION_COUNT)
                .build();
        // 设置超时时间
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(TIME_OUT)
                .setConnectionRequestTimeout(TIME_OUT)
                .setSocketTimeout(TIME_OUT)
                .build();
        // 设置Http连接池
        PoolingHttpClientConnectionManager pcm = new PoolingHttpClientConnectionManager();
        pcm.setDefaultMaxPerRoute(COUNT);
        pcm.setMaxTotal(TOTAL_COUNT);

        return HttpClientBuilder.create()
                .setServiceUnavailableRetryStrategy(strategy)
                .setRetryHandler(retryHandler)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(pcm)
                .build();
//        return HttpClientBuilder.create().build();
    }

    public static HttpRequestBase getRequest(String method,
                                             String requestBody,
                                             String header,
                                             String url) {
        LOG.debug("current request url: {}  current method:{} \n", url, method);
        HttpRequestBase request = null;

        if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            request = new HttpGet(url);
        } else if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            HttpPost post = new HttpPost(url);
            post.setEntity(getEntityData(requestBody));
            request = post;
        } else {
            throw new RuntimeException("Unsupported method:" + method);
        }
        Map<String, String> headerMap = gson.fromJson(header, Map.class);
        for (Map.Entry<String, String> entry : headerMap.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        return request;
    }

    public static void closeClient(CloseableHttpClient httpClient) {
        try {
            httpClient.close();
        } catch (IOException e) {
            throw new RuntimeException("close client error");
        }
    }

    /**
     * @param body 为json字符串
     * @return
     */
    public static StringEntity getEntityData(String body) {
        StringEntity stringEntity = new StringEntity(body, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding(StandardCharsets.UTF_8.name());
        return stringEntity;
    }
}

class MyServiceUnavailableRetryStrategy implements ServiceUnavailableRetryStrategy {
    private int executionCount;
    private long retryInterval;

    public MyServiceUnavailableRetryStrategy(MyServiceUnavailableRetryStrategy.Builder builder) {
        this.executionCount = builder.executionCount;
        this.retryInterval = builder.retryInterval;
    }

    @Override
    public boolean retryRequest(HttpResponse httpResponse, int executionCount, HttpContext httpContext) {
        int successCode = 200;
        return httpResponse.getStatusLine().getStatusCode() != successCode
                && executionCount < this.executionCount;
    }

    @Override
    public long getRetryInterval() {
        return this.retryInterval;
    }

    public static final class Builder {
        private int executionCount;
        private long retryInterval;

        public Builder() {
            executionCount = 5;
            retryInterval = 2000;
        }

        public MyServiceUnavailableRetryStrategy.Builder executionCount(int executionCount) {
            this.executionCount = executionCount;
            return this;
        }

        public MyServiceUnavailableRetryStrategy.Builder retryInterval(long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public MyServiceUnavailableRetryStrategy build() {
            return new MyServiceUnavailableRetryStrategy(this);
        }
    }
}

class MyHttpRequestRetryHandler implements HttpRequestRetryHandler {
    protected static final Logger LOG = LoggerFactory.getLogger(MyHttpRequestRetryHandler.class);

    private int executionMaxCount;

    public MyHttpRequestRetryHandler(MyHttpRequestRetryHandler.Builder builder) {
        this.executionMaxCount = builder.executionMaxCount;
    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        LOG.info("第" + executionCount + "次重试");

        if (executionCount >= this.executionMaxCount) {
            // Do not retry if over max retry count
            return false;
        }
        if (exception instanceof InterruptedIOException) {
            // Timeout
            return true;
        }
        if (exception instanceof UnknownHostException) {
            // Unknown host
            return true;
        }
        if (exception instanceof SSLException) {
            // SSL handshake exception
            return true;
        }
        if (exception instanceof NoHttpResponseException) {
            // No response
            return true;
        }

        HttpClientContext clientContext = HttpClientContext.adapt(context);
        HttpRequest request = clientContext.getRequest();
        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
        // Retry if the request is considered idempotent
        return !idempotent;
    }


    public static final class Builder {
        private int executionMaxCount;

        public Builder() {
            executionMaxCount = 5;
        }

        public MyHttpRequestRetryHandler.Builder executionCount(int executionCount) {
            this.executionMaxCount = executionCount;
            return this;
        }

        public MyHttpRequestRetryHandler build() {
            return new MyHttpRequestRetryHandler(this);
        }
    }
}








