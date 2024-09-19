// initialize_central_accounts.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main() {
    Account accounts[MAX_ACCOUNTS];
    srand(time(NULL));

    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        accounts[i].accountNumber = i + 1;
        accounts[i].departmentNumber = (i < 500) ? 1 : 2; // Assign first 500 to Department 1, next 500 to Department 2
        accounts[i].amount = (float)(rand() % 100000) / 100.0; // Random amount between 0.00 and 999.99
    }

    FILE *file = fopen("central_accounts.dat", "wb");
    if (!file) {
        perror("Error opening central_accounts.dat");
        return 1;
    }

    fwrite(accounts, sizeof(Account), MAX_ACCOUNTS, file);
    fclose(file);

    printf("central_accounts.dat initialized successfully with %d accounts.\n", MAX_ACCOUNTS);

    return 0;
}
