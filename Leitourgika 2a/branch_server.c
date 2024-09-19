// branch_server.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

// Mutex for each account to handle concurrent access
pthread_mutex_t account_mutex[TOTAL_ACCOUNTS + 1]; // accountNumber starts from 1

unsigned char branch_department;

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
    printf("Branch %d locked account %d\n", branch_department, accountNumber);
}

// Function to unlock an account
void unlock_account(int accountNumber) {
    if (accountNumber < 1 || accountNumber > TOTAL_ACCOUNTS) {
        return;
    }
    pthread_mutex_unlock(&account_mutex[accountNumber]);
    printf("Branch %d unlocked account %d\n", branch_department, accountNumber);
}

// Function to forward a request to the central server
void forward_to_central(Request *request, Response *response) {
    int central_sock;
    struct sockaddr_in central_address;

    if ((central_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed for central server");
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Central server connection failed.");
        return;
    }

    memset(&central_address, 0, sizeof(central_address));
    central_address.sin_family = AF_INET;
    central_address.sin_port = htons(CENTRAL_PORT);
    central_address.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(central_sock, (struct sockaddr *)&central_address, sizeof(central_address)) < 0) {
        perror("Connection to central server failed");
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Central server connection failed.");
        close(central_sock);
        return;
    }

    // Send request to central server
    send(central_sock, request, sizeof(Request), 0);

    // Receive response from central server
    recv(central_sock, response, sizeof(Response), 0);

    close(central_sock);
}

// Function to handle Display Query
void handle_display(int accountNumber, Response *response) {
    FILE *file = fopen("branch_accounts.dat", "rb");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open branch_accounts.dat.");
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

    fclose(file);
    // If not found locally, forward to central server
    Request forward_request = {
        .queryType = QUERY_DISPLAY,
        .accountNumber1 = accountNumber,
        .accountNumber2 = 0,
        .amount = 0.0,
        .departmentNumber = 0
    };
    forward_to_central(&forward_request, response);
}

// Function to handle Update Query
void handle_update(int accountNumber, float amount, Response *response) {
    // Lock account locally
    lock_account(accountNumber);

    // Lock account centrally and update
    Request central_request = {
        .queryType = QUERY_UPDATE,
        .accountNumber1 = accountNumber,
        .accountNumber2 = 0,
        .amount = amount,
        .departmentNumber = 0
    };
    Response central_response;
    forward_to_central(&central_request, &central_response);

    if (central_response.status == STATUS_SUCCESS) {
        // Update locally
        FILE *file = fopen("branch_accounts.dat", "r+b");
        if (!file) {
            response->status = STATUS_ERROR;
            snprintf(response->message, sizeof(response->message), "Unable to open branch_accounts.dat.");
            unlock_account(accountNumber);
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
                snprintf(response->message, sizeof(response->message), "Account %d updated locally. New balance: %.2f", accountNumber, account.amount);
                break;
            }
        }

        fclose(file);

        if (!found) {
            // Account exists centrally but not locally, add it
            FILE *f = fopen("branch_accounts.dat", "ab");
            if (f) {
                Account new_account = {accountNumber, branch_department, amount};
                fwrite(&new_account, sizeof(Account), 1, f);
                fclose(f);
                snprintf(response->message, sizeof(response->message), "Account %d added locally with balance: %.2f", accountNumber, amount);
                response->status = STATUS_SUCCESS;
            } else {
                snprintf(response->message, sizeof(response->message), "Failed to add account %d locally.", accountNumber);
                response->status = STATUS_ERROR;
            }
        }
    } else {
        // Central server failed to update
        snprintf(response->message, sizeof(response->message), "Central server failed to update account %d.", accountNumber);
        response->status = STATUS_ERROR;
    }

    // Unlock account locally
    unlock_account(accountNumber);
}

// Function to handle Transfer Query
void handle_transfer(int fromAccount, int toAccount, float amount, Response *response) {
    // Determine if both accounts belong to this branch
    int belongs_to_branch = 0;
    FILE *file = fopen("branch_accounts.dat", "rb");
    if (file) {
        Account account;
        while (fread(&account, sizeof(Account), 1, file)) {
            if (account.accountNumber == fromAccount || account.accountNumber == toAccount) {
                belongs_to_branch = 1;
                break;
            }
        }
        fclose(file);
    }

    // Lock accounts locally if they belong to this branch
    if (belongs_to_branch) {
        if (fromAccount < toAccount) {
            lock_account(fromAccount);
            lock_account(toAccount);
        } else {
            lock_account(toAccount);
            lock_account(fromAccount);
        }
    }

    // Forward transfer request to central server
    Request central_request = {
        .queryType = QUERY_TRANSFER,
        .accountNumber1 = fromAccount,
        .accountNumber2 = toAccount,
        .amount = amount,
        .departmentNumber = 0
    };
    Response central_response;
    forward_to_central(&central_request, &central_response);

    if (central_response.status == STATUS_SUCCESS) {
        // Update locally if accounts belong to this branch
        if (belongs_to_branch) {
            FILE *file = fopen("branch_accounts.dat", "r+b");
            if (!file) {
                response->status = STATUS_ERROR;
                snprintf(response->message, sizeof(response->message), "Unable to open branch_accounts.dat.");
                if (belongs_to_branch) {
                    unlock_account(fromAccount);
                    unlock_account(toAccount);
                }
                return;
            }

            Account account;
            int found_from = 0, found_to = 0;

            while (fread(&account, sizeof(Account), 1, file)) {
                if (account.accountNumber == fromAccount) {
                    fseek(file, -sizeof(Account), SEEK_CUR);
                    account.amount -= amount;
                    fwrite(&account, sizeof(Account), 1, file);
                    found_from = 1;
                } else if (account.accountNumber == toAccount) {
                    fseek(file, -sizeof(Account), SEEK_CUR);
                    account.amount += amount;
                    fwrite(&account, sizeof(Account), 1, file);
                    found_to = 1;
                }
            }

            fclose(file);

            snprintf(response->message, sizeof(response->message), "Transferred %.2f from account %d to account %d locally.", amount, fromAccount, toAccount);
            response->status = STATUS_SUCCESS;
        } else {
            // Transfer does not involve this branch's accounts
            snprintf(response->message, sizeof(response->message), "Transferred %.2f from account %d to account %d.", amount, fromAccount, toAccount);
            response->status = STATUS_SUCCESS;
        }
    } else {
        // Central server failed to process transfer
        snprintf(response->message, sizeof(response->message), "Central server failed to transfer %.2f from account %d to account %d.", amount, fromAccount, toAccount);
        response->status = STATUS_ERROR;
    }

    // Unlock accounts locally if they belong to this branch
    if (belongs_to_branch) {
        unlock_account(fromAccount);
        unlock_account(toAccount);
    }
}

// Function to handle Average Query
void handle_average(unsigned char departmentNumber, Response *response) {
    // If the department is not this branch's, forward to central server
    if (departmentNumber != branch_department) {
        Request central_request = {
            .queryType = QUERY_AVERAGE,
            .accountNumber1 = 0,
            .accountNumber2 = 0,
            .amount = 0.0,
            .departmentNumber = departmentNumber
        };
        forward_to_central(&central_request, response);
        return;
    }

    // Calculate average locally
    FILE *file = fopen("branch_accounts.dat", "rb");
    if (!file) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "Unable to open branch_accounts.dat.");
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

    fclose(file);

    if (count == 0) {
        response->status = STATUS_ERROR;
        snprintf(response->message, sizeof(response->message), "No accounts found for department %d.", departmentNumber);
        return;
    }

    float averageAmount = totalAmount / count;
    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", local);

    snprintf(response->message, sizeof(response->message), "Average amount for department %d: %.2f\nTimestamp: %s", departmentNumber, averageAmount, timestamp);
    response->status = STATUS_SUCCESS;
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

    // Determine if the request is for this branch
    int is_local_query = 0;
    if (request.queryType == QUERY_DISPLAY || request.queryType == QUERY_UPDATE || request.queryType == QUERY_TRANSFER) {
        // 80% chance to handle locally if the account belongs to this branch
        int random = rand() % 100;
        if (random < 80) {
            // Check if accountNumber1 belongs to this branch
            FILE *file = fopen("branch_accounts.dat", "rb");
            if (file) {
                Account account;
                while (fread(&account, sizeof(Account), 1, file)) {
                    if (account.accountNumber == request.accountNumber1) {
                        is_local_query = 1;
                        break;
                    }
                }
                fclose(file);
            }
        }
    } else if (request.queryType == QUERY_AVERAGE) {
        // Always handle average queries locally
        if (request.departmentNumber == branch_department) {
            is_local_query = 1;
        }
    }

    if (is_local_query) {
        // Handle query locally
        switch (request.queryType) {
            case QUERY_DISPLAY:
                handle_display(request.accountNumber1, &response);
                break;
            case QUERY_UPDATE:
                handle_update(request.accountNumber1, request.amount, &response);
                break;
            case QUERY_TRANSFER:
                handle_transfer(request.accountNumber1, request.accountNumber2, request.amount, &response);
                break;
            case QUERY_AVERAGE:
                handle_average(request.departmentNumber, &response);
                break;
            default:
                response.status = STATUS_ERROR;
                snprintf(response.message, sizeof(response.message), "Invalid query type.");
        }
    } else {
        // Forward query to central server
        switch (request.queryType) {
            case QUERY_DISPLAY:
            case QUERY_UPDATE:
            case QUERY_TRANSFER:
            case QUERY_AVERAGE:
                // Forward to central server
                {
                    // Create a socket to central server
                    int central_sock;
                    struct sockaddr_in central_address;

                    if ((central_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                        perror("Socket creation failed for central server");
                        response.status = STATUS_ERROR;
                        snprintf(response.message, sizeof(response.message), "Central server connection failed.");
                        break;
                    }

                    memset(&central_address, 0, sizeof(central_address));
                    central_address.sin_family = AF_INET;
                    central_address.sin_port = htons(CENTRAL_PORT);
                    central_address.sin_addr.s_addr = inet_addr("127.0.0.1");

                    if (connect(central_sock, (struct sockaddr *)&central_address, sizeof(central_address)) < 0) {
                        perror("Connection to central server failed");
                        response.status = STATUS_ERROR;
                        snprintf(response.message, sizeof(response.message), "Central server connection failed.");
                        close(central_sock);
                        break;
                    }

                    // Send request to central server
                    send(central_sock, &request, sizeof(Request), 0);

                    // Receive response from central server
                    recv(central_sock, &response, sizeof(Response), 0);

                    close(central_sock);
                }
                break;
            default:
                response.status = STATUS_ERROR;
                snprintf(response.message, sizeof(response.message), "Invalid query type.");
        }
    }

    // Send response back to client
    send(sock, &response, sizeof(Response), 0);
    close(sock);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <department_number (1 or 2)>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    branch_department = (unsigned char)atoi(argv[1]);
    if (branch_department < 1 || branch_department > DEPARTMENT_COUNT) {
        fprintf(stderr, "Invalid department number. Must be 1 or 2.\n");
        exit(EXIT_FAILURE);
    }

    initialize_mutexes();

    // Load local accounts from central accounts.dat
    FILE *central_file = fopen("accounts.dat", "rb");
    if (!central_file) {
        perror("Unable to open accounts.dat for loading branch accounts");
        exit(EXIT_FAILURE);
    }

    FILE *branch_file = fopen("branch_accounts.dat", "wb");
    if (!branch_file) {
        perror("Unable to create branch_accounts.dat");
        fclose(central_file);
        exit(EXIT_FAILURE);
    }

    Account account;
    while (fread(&account, sizeof(Account), 1, central_file)) {
        if (account.departmentNumber == branch_department) {
            fwrite(&account, sizeof(Account), 1, branch_file);
        }
    }

    fclose(central_file);
    fclose(branch_file);

    int server_fd;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Bind to BRANCH_PORT_BASE + department_number
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(BRANCH_PORT_BASE + branch_department);

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

    printf("Branch server for department %d listening on port %d\n", branch_department, BRANCH_PORT_BASE + branch_department);

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
