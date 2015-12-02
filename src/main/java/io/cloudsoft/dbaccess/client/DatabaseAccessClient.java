package io.cloudsoft.dbaccess.client;

import javax.annotation.Nullable;

public interface DatabaseAccessClient {
    
    void createUser(String username, String password);
    void deleteUser(String username);
    String getJdbcUrl(@Nullable String username, @Nullable String password);
    
}
