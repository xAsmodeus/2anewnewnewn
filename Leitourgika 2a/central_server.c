// central_server.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

// Mutex for each account to handle concurrent access
pthread_mutex_t account_mutex[TOTAL_ACCOUNTS + 1]; // accountNumber starts from 1

// Function to initialize mutexes
void initialize_mutexes() {
    for (int i = 0; i <= TOTAL_ACCOUNTS; ++i) {
        pthread_mutex_init(&account_mutex[i], NULL);
    }
}

// Function to lock an account
void lock_account(int accountNumber) {
    if (accountNumber < 1 || accountNumber > TOTAL_ACCOUNTS) {
        return;
    }
    pthread_mutex_lock(&account_mutex[accountNumber]);
    printf("Central server locked account %d\n", accountNumber);
}

// Function to unlock an account
void unlock_account(int accountNumber) {
    if (accountNumber < 1 || accountNumber > TOTAL_ACCOUNTS) {
        return;
    }
    pthread_mutex_unlock(&account_mutex[accountNumber]);
    printf("Central server unlocked account %d\n", accountNumber);
}

// Function to handle Display Query
void handle_display(int accountNumber, Response *response) {
    FILE *file = fopen("accounts.dat", "rb");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open accounts.dat.");
        return;
    }

    Account account;
    while (fread(&account, sizeof(Account), 1, file)) {
        if (account.accountNumber == accountNumber) {
            snprintf(response->message, sizeof(response->message), "Account %d balance: %.2f", accountNumber, account.amount);
            response->status = STATUS_SUCCESS;
            fclose(file);
            return;
        }
    }

    response->status = STATUS_ERROR;
    snprintf(response->message, sizeof(response->message), "Account %d not found.", accountNumber);
    fclose(file);
}

// Function to handle Update Query
void handle_update(int accountNumber, float amount, Response *response) {
    FILE *file = fopen("accounts.dat", "r+b");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open accounts.dat.");
        return;
    }

    Account account;
    int found = 0;

    while (fread(&account, sizeof(Account), 1, file)) {
        if (account.accountNumber == accountNumber) {
            fseek(file, -sizeof(Account), SEEK_CUR);
            account.amount += amount;
            fwrite(&account, sizeof(Account), 1, file);
            found = 1;
            snprintf(response->message, sizeof(response->message), "Account %d updated. New balance: %.2f", accountNumber, account.amount);
            break;
        }
    }

    if (!found) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Account %d not found.", accountNumber);
    } else {
        response->status = STATUS_SUCCESS;
    }

    fclose(file);
}

// Function to handle Transfer Query
void handle_transfer(int fromAccount, int toAccount, float amount, Response *response) {
    if (fromAccount == toAccount) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Cannot transfer to the same account.");
        return;
    }

    FILE *file = fopen("accounts.dat", "r+b");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open accounts.dat.");
        return;
    }

    Account from, to;
    int found_from = 0, found_to = 0;
    long from_offset, to_offset;

    // Find fromAccount
    while (fread(&from, sizeof(Account), 1, file)) {
        if (from.accountNumber == fromAccount) {
            from_offset = ftell(file) - sizeof(Account);
            found_from = 1;
            break;
        }
    }

    // Find toAccount
    fseek(file, 0, SEEK_SET);
    while (fread(&to, sizeof(Account), 1, file)) {
        if (to.accountNumber == toAccount) {
            to_offset = ftell(file) - sizeof(Account);
            found_to = 1;
            break;
        }
    }

    if (!found_from || !found_to) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "One or both accounts not found.");
        fclose(file);
        return;
    }

    if (from.amount < amount) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Insufficient funds in account %d.", fromAccount);
        fclose(file);
        return;
    }

    // Deduct from fromAccount
    fseek(file, from_offset, SEEK_SET);
    from.amount -= amount;
    fwrite(&from, sizeof(Account), 1, file);

    // Add to toAccount
    fseek(file, to_offset, SEEK_SET);
    to.amount += amount;
    fwrite(&to, sizeof(Account), 1, file);

    snprintf(response->message, sizeof(response->message), "Transferred %.2f from account %d to account %d.", amount, fromAccount, toAccount);
    response->status = STATUS_SUCCESS;

    fclose(file);
}

// Function to handle Average Query
void handle_average(unsigned char departmentNumber, Response *response) {
    FILE *file = fopen("accounts.dat", "rb");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open accounts.dat.");
        return;
    }

    Account account;
    float totalAmount = 0;
    int count = 0;

    while (fread(&account, sizeof(Account), 1, file)) {
        if (account.departmentNumber == departmentNumber) {
            totalAmount += account.amount;
            count++;
        }
    }

    if (count == 0) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "No accounts found for department %d.", departmentNumber);
        fclose(file);
        return;
    }

    float averageAmount = totalAmount / count;
    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", local);

    snprintf(response->message, sizeof(response->message), "Average amount for department %d: %.2f\nTimestamp: %s", departmentNumber, averageAmount, timestamp);
    response->status = STATUS_SUCCESS;

    fclose(file);
}

// Function to handle each client connection
void *handle_client(void *client_socket_ptr) {
    int sock = *((int *)client_socket_ptr);
    free(client_socket_ptr);

    Request request;
    Response response;
    memset(&request, 0, sizeof(Request));
    memset(&response, 0, sizeof(Response));

    // Receive request
    if (recv(sock, &request, sizeof(Request), 0) <= 0) {
        perror("recv failed");
        close(sock);
        pthread_exit(NULL);
    }

    // Process request based on query type
    switch (request.queryType) {
        case QUERY_DISPLAY:
            handle_display(request.accountNumber1, &response);
            break;
        case QUERY_UPDATE:
            lock_account(request.accountNumber1);
            handle_update(request.accountNumber1, request.amount, &response);
            unlock_account(request.accountNumber1);
            break;
        case QUERY_TRANSFER:
            // To prevent deadlocks, always lock in ascending order
            if (request.accountNumber1 < request.accountNumber2) {
                lock_account(request.accountNumber1);
                lock_account(request.accountNumber2);
            } else {
                lock_account(request.accountNumber2);
                lock_account(request.accountNumber1);
            }
            handle_transfer(request.accountNumber1, request.accountNumber2, request.amount, &response);
            unlock_account(request.accountNumber1);
            unlock_account(request.accountNumber2);
            break;
        case QUERY_AVERAGE:
            handle_average(request.departmentNumber, &response);
            break;
        default:
            response.status = STATUS_ERROR;
            snprintf(response.message, sizeof(response.message), "Invalid query type.");
    }

    // Send response
    send(sock, &response, sizeof(Response), 0);
    close(sock);
    pthread_exit(NULL);
}

int main() {
    initialize_mutexes();

    int server_fd;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Bind to CENTRAL_PORT
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(CENTRAL_PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // Listen
    if (listen(server_fd, 10) < 0) {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Central server listening on port %d\n", CENTRAL_PORT);

    // Accept clients
    while (1) {
        int *new_sock = malloc(sizeof(int));
        if ((*new_sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept failed");
            free(new_sock);
            continue;
        }

        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, (void *)new_sock) != 0) {
            perror("pthread_create failed");
            free(new_sock);
        }

        pthread_detach(tid);
    }

    // Cleanup (unreachable in this example)
    close(server_fd);
    for (int i = 0; i <= TOTAL_ACCOUNTS; ++i) {
        pthread_mutex_destroy(&account_mutex[i]);
    }

    return 0;
}
