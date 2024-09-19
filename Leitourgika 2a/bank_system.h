// bank_system.h
#ifndef BANK_SYSTEM_H
#define BANK_SYSTEM_H

#include <pthread.h>
#include <time.h>

// Constants
#define CENTRAL_PORT 9000
#define BRANCH_PORT_BASE 9100
#define DEPARTMENT_COUNT 2
#define TOTAL_ACCOUNTS 1000
#define ACCOUNTS_PER_DEPARTMENT (TOTAL_ACCOUNTS / DEPARTMENT_COUNT)

// Error codes
#define STATUS_SUCCESS 0
#define STATUS_ERROR 1

// Query types
#define QUERY_DISPLAY 1
#define QUERY_UPDATE 2
#define QUERY_TRANSFER 3
#define QUERY_AVERAGE 4

// Account record structure
typedef struct {
    int accountNumber;
    unsigned char departmentNumber;
    float amount;
} Account;

// Request structure
typedef struct {
    int queryType;
    int accountNumber1; // Used for Display, Update, and Transfer (fromAccount)
    int accountNumber2; // Used for Transfer (toAccount)
    float amount;       // Used for Update and Transfer
    unsigned char departmentNumber; // Used for Average
} Request;

// Response structure
typedef struct {
    int status;
    char message[256];
} Response;

#endif // BANK_SYSTEM_H
