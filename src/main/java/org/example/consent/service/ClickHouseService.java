package org.example.consent.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseService {

    @Value("${clickhouse.url:jdbc:clickhouse://localhost:8123/default}")
    private String url;
    
    @Value("${clickhouse.user:default}")
    private String user;
    
    @Value("${clickhouse.password:password}")
    private String password;

    public List<Map<String, Object>> fetchData(long requiredMask) {
        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT * FROM athlete_data_gold WHERE bitAnd(consent_mask, ?) = ?";
        
        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, requiredMask);
            pstmt.setLong(2, requiredMask);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new java.util.HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    public void executeUpdate(String sql, Object... params) {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            
            pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
