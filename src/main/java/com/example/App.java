package com.example;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProvider;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;
import io.camunda.zeebe.client.ZeebeClientBuilder;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import java.time.Duration;
import java.util.Scanner;


public class App 
{
    private static final String zeebeAPI = "67919a7b-ab86-4601-a9a3-31eeb2ec470f.bru-2.zeebe.camunda.io:443";
    private static final String audience = "zeebe.camunda.io";
    private static final String clientId = "~9Y1aZ77xW0H.cKwxYCUaLqUtg4F9Wnv";
    private static final String clientSecret = "rqn1K1dTBe5p8xMn1rrI~p7t5qq8ZXcPnfDE6JpziMCUKjIcD0a-YRa_ZG6KKP6p";
    private static final String oAuthAPI = "https://login.cloud.camunda.io/oauth/token";

    public static void main(String[] args) {
        OAuthCredentialsProvider credentialsProvider =
            new OAuthCredentialsProviderBuilder()
                .authorizationServerUrl(oAuthAPI)
                .audience(audience)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

        try (ZeebeClient client = ZeebeClient.newClientBuilder()
                .gatewayAddress(zeebeAPI)
                .credentialsProvider(credentialsProvider)
                .build()) {
            client.newTopologyRequest().send().join();
            System.out.println("Zeebe topology: " + client.newTopologyRequest().send().join().toString());
        }

        final String jobType = "foo";
        final ZeebeClientBuilder clientBuilder;
        try (final ZeebeClient client = clientBuilder.build()) {

            System.out.println("Opening job worker.");

            try (final JobWorker workerRegistration =
                client
                    .newWorker()
                    .jobType(jobType)
                    .handler(new ExampleJobHandler())
                    .timeout(Duration.ofSeconds(10))
                    .open()) {
                System.out.println("Job worker opened and receiving jobs.");

                
            }
        }
    }

    private static class ExampleJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {
            // here: business logic that is executed with every job
            System.out.println(job);
            client.newCompleteCommand(job.getKey()).send().join();
        }
    }
        
    
}
