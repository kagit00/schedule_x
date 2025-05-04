package com.shedule.x.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class OpenAPIConfig {
    /**
     * Custom open api open api.
     *
     * @return the open api
     */
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .addSecurityItem(new SecurityRequirement()
                        .addList("JWT") // Accept JWT
                        .addList("OAuth2")) // Or accept OAuth2
                .components(new Components()
                        .addSecuritySchemes("JWT",
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.HTTP)
                                        .scheme("bearer")
                                        .bearerFormat("JWT")
                                        .description("Provide the JWT token in the format: Bearer {token}"))
                        .addSecuritySchemes("OAuth2",
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.OAUTH2)
                                        .description("OAuth2 authentication using Google")
                                        .flows(new OAuthFlows()
                                                .authorizationCode(new OAuthFlow()
                                                        .authorizationUrl("https://accounts.google.com/o/oauth2/auth")
                                                        .tokenUrl("https://oauth2.googleapis.com/token")
                                                        .scopes(new Scopes().addString("read", "Read access"))))));
    }
}
