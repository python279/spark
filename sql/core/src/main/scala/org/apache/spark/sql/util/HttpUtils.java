package org.apache.spark.sql.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class HttpUtils {
    static Logger log = LogManager.getLogger(HttpUtils.class);

    public static String httpReuquest(String url, String requestType, String data) throws Exception {
        URL restServiceURL;
        HttpURLConnection httpURLConnection = null;
        StringBuilder sb = new StringBuilder();
        InputStream is = null;
        BufferedReader reader = null;
        long stime= new Date().getTime();
        try {
            restServiceURL = new URL(url);
            httpURLConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpURLConnection.setRequestProperty("content-Type", "application/json;charset=UTF-8");
            httpURLConnection.setConnectTimeout(2000);//设置连接主机服务器的超时时间:2000毫秒
            httpURLConnection.setReadTimeout(6000);//设置读取远程返回数据时间:6000毫秒
            if(requestType.equals("POST")){
                httpURLConnection.setRequestMethod("POST");
                httpURLConnection.setDoOutput(true);
                httpURLConnection.setDoInput(true);
                httpURLConnection.setUseCaches(false);
                httpURLConnection.setRequestProperty("Content-Length",String.valueOf(data.getBytes().length));
                OutputStream outputStream = httpURLConnection.getOutputStream();
                outputStream.write(data.getBytes());
                //outputStream.flush();
                outputStream.close();
            }else if(requestType.equals("GET")) {
                httpURLConnection.setRequestMethod("GET");
            }

            if (HttpURLConnection.HTTP_OK == httpURLConnection.getResponseCode()) {
                is = httpURLConnection.getInputStream();
                reader = new BufferedReader((new InputStreamReader(is, StandardCharsets.UTF_8)));
            } else {
                is = httpURLConnection.getErrorStream();
                reader = new BufferedReader((new InputStreamReader(is, "utf-8")));
            }
            //httpURLConnection.disconnect();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            long endTime = new Date().getTime();
            log.info("url:"+url.toString()+"consume time:"+(endTime-stime)+" ms");
            return sb.toString();
        } catch (IOException e) {
            log.error("HttpUtils request url :" + url + ";request type :'" + requestType + "' error:"+e.getMessage());
            throw  new Exception("HttpUtils request url :" + url + ";request type :" + requestType + " error:"+e.getMessage());
        } finally {
            //关闭连接
            if (reader != null ) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (httpURLConnection != null) {
                httpURLConnection.disconnect();
            }
        }
    }

    public static String sendRequest(String url, String urlSuffix, String reqType, String db,String sqlText,String token) {
        List<String> urlList = Arrays.asList(url.split(","));
        Collections.shuffle(urlList);
        List urlShuffleList = new ArrayList(urlList);
        String result = null;
        String authSql = null;
        if (!urlShuffleList.isEmpty()) {
            try{
                String newUrl = urlShuffleList.get(0).toString();
                Map<String,String> map=new HashMap<String,String>();
                if(reqType.equals("POST")) {
                    map.put("source","sparksql");
                    map.put("username",token);
                    map.put("currentdb",db);
                    map.put("sql",sqlText);

                    ObjectMapper objectMapper = new ObjectMapper();
                    String userMapJson = objectMapper.writeValueAsString(map);
                    JsonNode node = objectMapper.readTree(userMapJson);
                    authSql = node.toString();
                }
                result = httpReuquest(newUrl + urlSuffix,reqType,authSql);
            }catch (Exception e) {
                urlShuffleList.remove(0);
                e.printStackTrace();
            }
        }
        return result;
    }

    public static String checkStatus(String authorityUrl,String sqlText,String token) throws Exception{
        String status = null;
        String checkState = null;
        String logs = null;
        String errorInfo = null;
        String submitUser = null;
        if(!StringUtils.isEmpty(token)) {
           String decryptUser = AESUtils.decrypt(token);
           if (decryptUser == null || !decryptUser.startsWith("DSP")) {
               throw new Exception("invalid spark.submit.user.token");
           }
           String[] userArray = decryptUser.split("\\_");
           if (userArray.length == 3) {
               submitUser = userArray[1];
           } else {
               submitUser = decryptUser.replace("DSP_", "");
           }
        }
        String result = HttpUtils.sendRequest(authorityUrl, "/checksql", "POST", "default", sqlText, submitUser);
        if (result.contains("\"key\":\"")) {
            String key = HttpUtils.getJsonValue(result, "key");
            int count = 0;
            while ( count < 30 ) {
                checkState = HttpUtils.sendRequest(authorityUrl, "/getstate/" + key, "GET", "", "", "");

                if (checkState.equals("{\"status\":\"error\"}")) {
                    logs = sendRequest(authorityUrl, "/getlog/" + key, "GET", "", "", "");
                    if (Pattern.matches("^(?i)drop\\s+(table|view)\\s+if\\s+exists\\s+.*",sqlText) && logs.contains("没有找到表")) {
                        log.error("check sql error: " + logs);
                        return status;
                    } else {
                        errorInfo = "UM: " + submitUser + ", Message: " + logs + ", key: " + key;
                        break;
                    }
                } else if (checkState.equals("{\"status\":\"done\"}")) {//success
                    status=key;
                    return status;
                }
                try {
                    if(count<5) {
                        Thread.sleep(1000);
                    } else {
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                count++;
            }

            if (errorInfo == null) {
                throw new Exception("check sql timeout, key: " + key);
            } else {
                throw new Exception(errorInfo);
            }
        } else {
            throw new Exception("check sql fail, response data: " + result);
        }
    }

    public static String getJsonValue(String json,String key) {
        ObjectMapper objectMapper = new ObjectMapper() ;
        Map<String,String> maps = null;
        try {
            maps = objectMapper.readValue(json, Map.class);
        } catch (IOException e) {
            log.error("getJsonValue: json to map exception: " + json);
        }
        return maps.get(key).toString();
    }
}
