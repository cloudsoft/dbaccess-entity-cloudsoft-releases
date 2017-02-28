package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.entity.stock.BasicApplicationImpl;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public abstract class DatabaseAccessEntityImpl extends BasicApplicationImpl implements DatabaseAccessEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessEntityImpl.class);

    @Override
    public void init() {
        super.init();
        String url = config().get(ENDPOINT_URL);
        URI u = null;
        if (Strings.isNonBlank(url)) {
            u = URI.create(url);
        }
        
        confirmConfigSetAndMatches(PROTOCOL_SCHEME, u!=null ? u.getScheme() : null, getProtocolScheme());
        confirmConfigSetAndMatches(HOST, u!=null ? u.getHost() : null);
        confirmConfigSetAndMatches(PORT, u!=null ? u.getPort()>=0 ? ""+u.getPort() : null : null);
        confirmConfigSetAndMatches(ADMIN_USER, getUserFromUrl(u), getUrlQueryParam(u, "user"));
        confirmConfigSetAndMatches(ADMIN_PASSWORD, getPasswordFromUrl(u), getUrlQueryParam(u, "password"));
        
        if (u!=null) {
            String path = u.getPath();
            path = Strings.removeAllFromStart(path, "/");
            path = Strings.removeAllFromEnd(path, "/");
            if (Strings.isNonBlank(path)) {
                confirmConfigSetAndMatches(DATABASE, path);
            }
        }
    }

    public abstract String getProtocolScheme();
    
    private static String getUserFromUrl(URI u) {
        if (u==null) return null;
        String u1 = u.getUserInfo();
        if (Strings.isBlank(u1)) return null;
        if (u1.indexOf(':')>=0) return u1.substring(0, u1.indexOf(':'));
        else return u1;
    }
    
    private static String getPasswordFromUrl(URI u) {
        if (u==null) return null;
        String u1 = u.getUserInfo();
        if (Strings.isBlank(u1)) return null;
        if (u1.indexOf(':')>=0) return u1.substring(u1.indexOf(':')+1);
        else return null;
    }

    // based on http://stackoverflow.com/questions/13592236/parse-the-uri-string-into-name-value-collection-in-java
    private static Map<String, String> splitQuery(URI url) {
        Map<String, String> queryParams = new LinkedHashMap<String, String>();
        if (url==null) return queryParams;
        String query = url.getQuery();
        if (query==null) return queryParams;
        
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            try {
                queryParams.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                // silently ignore this param
            }
        }
        return queryParams;
    }
    
    private static String getUrlQueryParam(URI u, String param) {
        if (u==null) return null;
        return splitQuery(u).get(param);
    }
    
    private void confirmConfigSetAndMatches(ConfigKey<String> key, String ...uVals) {
        String oVal = config().get(key);
        String val = oVal;
        for (String uVal: uVals) {
            if (uVal!=null) {
                if (val!=null) {
                    if (!uVal.equals(val)) {
                        throw new IllegalArgumentException("Incompatible values for "+key+": "+val+" "+uVal);
                    }
                } else {
                    val = uVal;
                }
            }
        }
        if (val==null) {
            throw new IllegalArgumentException("Config "+key+" is required explicitly or inferrable from "+ENDPOINT_URL.getName());
        }
        if (oVal==null) {
            config().set(key, val);
        }
    }

    protected String getCloudFoundryFormatDatastoreUrl(String username, String password) {
        // always this format, as per https://github.com/cloudfoundry-samples/spring-music
        // (even though oracle sometimes uses user/password; we apply that in jdbc url
        // but not here)
        StringBuilder result = new StringBuilder(config().get(PROTOCOL_SCHEME)+"://");
        if (username!=null) {
            result.append(Urls.encode(username));
            result.append(":");
            if (password!=null) {
                result.append(Urls.encode(password));
            }
            result.append("@");
        }
        result.append(config().get(HOST));
        if (Strings.isNonBlank(config().get(PORT))) {
            result.append(":");
            result.append(config().get(PORT));
        }
        result.append("/");
        if (Strings.isNonBlank(config().get(DATABASE))) {
            result.append(Urls.encode(config().get(DATABASE)));
        }
        return result.toString();
    }

    @Override
    protected void doStart(Collection<? extends Location> locations) {
        createUser(false);
        super.doStart(locations);
    }
    
    @Override
    public void stop() {
        deleteUser(false);
        super.stop();
    }
    
    public void createUser(final boolean failIfUserExists) {
        // wrap in task so it shows up in UI; returns a status message
        DynamicTasks.queueIfPossible( Tasks.<String>builder().displayName("create user").body(new Callable<String>() {
            public String call() {
                String existingUser = sensors().get(USERNAME);
                if (existingUser!=null) {
                    if (failIfUserExists)
                        throw new IllegalStateException("User already defined; cannot create user again");
                    return "skipped: already have a user '"+existingUser+"'";
                } else {
                    String username = config().get(USERNAME);
                    String password = config().get(PASSWORD);
                    if (username == null) {
                        username = ("user_" + Identifiers.makeRandomJavaId(6)).toLowerCase();
                    }
                    if (password == null) {
                        password = Identifiers.makeRandomJavaId(12);
                    }
                    sensors().set(USERNAME, username);
                    sensors().set(PASSWORD, password);

                    String database = config().get(DATABASE);
                    DatabaseAccessEntityImpl.this.setDisplayName(String.format("DBAccess (%s%s - %s): %s",
                        Strings.isNonBlank(config().get(DEFAULT_DISPLAY_NAME)) ?
                            config().get(DEFAULT_DISPLAY_NAME)+" - " : "",
                        Strings.isNonBlank(database) ? database : "no DB", 
                        config().get(ACCESS_MODE),
                        username));

                    DatabaseAccessClient client = createClient();
                    LOG.info("Creating user for "+DatabaseAccessEntityImpl.this.getDisplayName()+" (at "+client.getAdminJdbcUrlForInfo()+")");
                    client.createUser(username, password);

                    exportCfSensorsSet(username, password, client.getJdbcUrl(username, password));
                    
                    return "created: '"+username+"'";
                }
            }
        }).build() ).orSubmitAsync(this).andWaitForSuccess();
    }

    @VisibleForTesting
    void exportCfSensorsSet(String username, String password, String jdbcUrl) {
        sensors().set(DATASTORE_URL, getCloudFoundryFormatDatastoreUrl(username, password) );
        sensors().set(CF_EXPORT_URI, getCloudFoundryFormatDatastoreUrl(username, password) );
        sensors().set(CF_EXPORT_JDBC_URL, jdbcUrl);
        
        sensors().set(CF_EXPORT_USERNAME, username );
        sensors().set(CF_EXPORT_PASSWORD, password );
        
        sensors().set(CF_EXPORT_HOST, config().get(HOST) );
        sensors().set(CF_EXPORT_PORT, config().get(PORT) );
        sensors().set(CF_EXPORT_DBNAME, config().get(DATABASE) );
    }
    
    @VisibleForTesting
    void exportCfSensorsCleared() {
        sensors().set(DATASTORE_URL, null);
        sensors().set(CF_EXPORT_URI, null);
        sensors().set(CF_EXPORT_JDBC_URL, null);
        
        sensors().set(CF_EXPORT_USERNAME, null);
        sensors().set(CF_EXPORT_PASSWORD, null);
        
        sensors().set(CF_EXPORT_HOST, null);
        sensors().set(CF_EXPORT_PORT, null);
        sensors().set(CF_EXPORT_DBNAME, null);
    }
    
    public void deleteUser(boolean failIfUserDoesNotExist) {
        String existingUser = sensors().get(USERNAME);
        if (existingUser==null) {
            if (failIfUserDoesNotExist)
                throw new IllegalStateException("User not defined; cannot delete user. Manually delete and reset sensor value to null.");
            return;
        }
        
        exportCfSensorsCleared();
        DatabaseAccessClient client = createClient();
        client.deleteUser(existingUser);
    }
}
