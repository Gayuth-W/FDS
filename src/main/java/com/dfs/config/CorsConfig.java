package com.dfs.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Permissive CORS for the local React dashboard.
 *
 * The React app runs in the browser and calls each node directly (cross-origin,
 * e.g. from http://localhost:8501 to http://localhost:8001). This is a local
 * developer/demo tool, so all origins are allowed; lock this down before any
 * non-local deployment.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*");
    }
}
