// generate_data.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define FILE_NAME "accounts.dat"

int main() {
    FILE *file = fopen(FILE_NAME, "wb");
    if (!file) {
        perror("Unable to open file");
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));

    for (int department = 1; department <= DEPARTMENT_COUNT; ++department) {
        for (int i = 0; i < ACCOUNTS_PER_DEPARTMENT; ++i) {
            Account account;
            account.accountNumber = (department - 1) * ACCOUNTS_PER_DEPARTMENT + i + 1;
            account.departmentNumber = department;
            account.amount = ((float)(rand() % 100000)) / 100.0; // Random amount between 0.00 and 999.99
            fwrite(&account, sizeof(Account), 1, file);
        }
    }

    fclose(file);
    printf("Data generation complete. Created '%s' with %d records.\n", FILE_NAME, TOTAL_ACCOUNTS);
    return 0;
}
