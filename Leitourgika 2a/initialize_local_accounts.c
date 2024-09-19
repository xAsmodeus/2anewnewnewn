// initialize_local_accounts.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <Department Number>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int department_number = atoi(argv[1]);
    if (department_number < 1 || department_number > MAX_DEPARTMENTS) {
        printf("Invalid Department Number. Must be between 1 and %d.\n", MAX_DEPARTMENTS);
        exit(EXIT_FAILURE);
    }

    Account central_accounts[MAX_ACCOUNTS];
    FILE *central_file = fopen("central_accounts.dat", "rb");
    if (!central_file) {
        perror("Failed to open central_accounts.dat");
        exit(EXIT_FAILURE);
    }
    size_t read_count = fread(central_accounts, sizeof(Account), MAX_ACCOUNTS, central_file);
    if (read_count != MAX_ACCOUNTS) {
        perror("Failed to read all accounts from central_accounts.dat");
        fclose(central_file);
        exit(EXIT_FAILURE);
    }
    fclose(central_file);

    Account local_accounts[MAX_ACCOUNTS];
    memset(local_accounts, 0, sizeof(local_accounts));

    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        if (central_accounts[i].departmentNumber == department_number) {
            local_accounts[i] = central_accounts[i];
        }
    }

    char filename[50];
    snprintf(filename, sizeof(filename), "local_accounts_dept_%d.dat", department_number);
    FILE *local_file = fopen(filename, "wb");
    if (!local_file) {
        perror("Failed to create local accounts file");
        exit(EXIT_FAILURE);
    }
    size_t write_count = fwrite(local_accounts, sizeof(Account), MAX_ACCOUNTS, local_file);
    if (write_count != MAX_ACCOUNTS) {
        perror("Failed to write all accounts to local_accounts file");
        fclose(local_file);
        exit(EXIT_FAILURE);
    }
    fclose(local_file);

    printf("%s initialized successfully.\n", filename);
    return 0;
}
