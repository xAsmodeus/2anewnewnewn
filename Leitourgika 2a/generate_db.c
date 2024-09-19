// generate_db.c
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define NUM_RECORDS 1000
#define DEPARTMENT_1 1
#define DEPARTMENT_2 2

typedef struct {
    int accountNumber;
    unsigned char departmentNumber;
    float amount;
} Account;

int main() {
    FILE *file = fopen("department_accounts.dat", "wb");
    if (file == NULL) {
        perror("Error opening file");
        return EXIT_FAILURE;
    }

    srand(time(NULL));

    for (int i = 0; i < NUM_RECORDS; i++) {
        Account account;
        account.accountNumber = i + 1;
        account.departmentNumber = (i < 500) ? DEPARTMENT_1 : DEPARTMENT_2;
        account.amount = (float)(rand() % 10000) / 100.0f;  // Random amount between 0.00 and 99.99

        fwrite(&account, sizeof(Account), 1, file);
    }

    fclose(file);
    printf("Database file created successfully.\n");
    return EXIT_SUCCESS;
}
