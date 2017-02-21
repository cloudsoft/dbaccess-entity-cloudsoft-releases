package io.cloudsoft.dbaccess.client;

import javax.annotation.Nullable;

public interface DatabaseAccessClient {
    
    void createUser(String username, String password);
    void deleteUser(String username);
    public void execute(String command, String usernameToReplace, String passwordToReplace);
    
    String getAdminJdbcUrlForInfo();
    String getJdbcUrl(@Nullable String username, @Nullable String password);
    
}
