// process_load.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

// Function to handle each request
void *handle_request(void *arg) {
    Request *request = (Request *)arg;
    Response response;
    memset(&response, 0, sizeof(Response));

    int sockfd;
    struct sockaddr_in servaddr;

    // Determine the server to connect to
    int departmentNumber = request->departmentNumber;
    int port;
    if (departmentNumber == 1 || departmentNumber == 2) {
        port = BRANCH_PORT_BASE + departmentNumber;
    } else {
        // For queries that don't target a specific department (e.g., transfers), connect to central
        port = CENTRAL_PORT;
    }

    // Create socket
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        pthread_exit(NULL);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    // Connect to server
    if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("Connection failed");
        close(sockfd);
        pthread_exit(NULL);
    }

    // Send request
    send(sockfd, request, sizeof(Request), 0);

    // Receive response
    recv(sockfd, &response, sizeof(Response), 0);

    // Optional: Print response
    // printf("Response: %s\n", response.message);

    close(sockfd);
    pthread_exit(NULL);
}

// Function to read load file and process requests
void process_load_file(const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Unable to open load file");
        exit(EXIT_FAILURE);
    }

    Request request;
    pthread_t threads[100];
    int thread_count = 0;

    while (fread(&request, sizeof(Request), 1, file)) {
        Request *req = malloc(sizeof(Request));
        memcpy(req, &request, sizeof(Request));

        if (pthread_create(&threads[thread_count], NULL, handle_request, (void *)req) != 0) {
            perror("pthread_create failed");
            free(req);
            continue;
        }

        thread_count++;

        if (thread_count == 100) {
            for (int i = 0; i < thread_count; ++i) {
                pthread_join(threads[i], NULL);
            }
            thread_count = 0;
        }
    }

    // Join remaining threads
    for (int i = 0; i < thread_count; ++i) {
        pthread_join(threads[i], NULL);
    }

    fclose(file);
    printf("Processed load file '%s'\n", filename);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <department_number> <load_file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int departmentNumber = atoi(argv[1]);
    const char *load_file = argv[2];

    // Validate department number
    if (departmentNumber != 1 && departmentNumber != 2) {
        fprintf(stderr, "Invalid department number. Must be 1 or 2.\n");
        exit(EXIT_FAILURE);
    }

    process_load_file(load_file);

    return 0;
}
