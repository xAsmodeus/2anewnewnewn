// client.c
#include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>

// Function to generate a random float between min and max
float random_float(float min, float max) {
    float scale = rand() / (float) RAND_MAX;
    return min + scale * (max - min);
}

// Function to generate a single request based on probabilities
Request generate_request(unsigned char departmentNumber) {
    Request request;
    memset(&request, 0, sizeof(Request));
    request.departmentNumber = departmentNumber;

    int rand_percent = rand() % 100;

    if (rand_percent < 35) {
        // Type 1: Display
        request.queryType = QUERY_DISPLAY;
        // Random accountNumber between 1 and 1000
        request.accountNumber1 = rand() % TOTAL_ACCOUNTS + 1;
    } else if (rand_percent < 70) {
        // Type 2: Update
        request.queryType = QUERY_UPDATE;
        request.accountNumber1 = rand() % TOTAL_ACCOUNTS + 1;
        // Random amount between -500.00 and +500.00
        request.amount = random_float(-500.0, 500.0);
    } else if (rand_percent < 95) {
        // Type 3: Transfer
        request.queryType = QUERY_TRANSFER;
        request.accountNumber1 = rand() % TOTAL_ACCOUNTS + 1;
        do {
            request.accountNumber2 = rand() % TOTAL_ACCOUNTS + 1;
        } while (request.accountNumber2 == request.accountNumber1);
        request.amount = random_float(1.0, 1000.0);
    } else {
        // Type 4: Average
        request.queryType = QUERY_AVERAGE;
        request.departmentNumber = departmentNumber;
    }

    return request;
}

// Function to write load file for a department
void generate_load_file(int departmentNumber, int requestCount, const char *filename) {
    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("Unable to create load file");
        exit(EXIT_FAILURE);
    }

    srand(time(NULL) + departmentNumber); // Seed differently for each department

    for (int i = 0; i < requestCount; ++i) {
        Request req = generate_request(departmentNumber);
        fwrite(&req, sizeof(Request), 1, file);
    }

    fclose(file);
    printf("Load file '%s' created with %d requests.\n", filename, requestCount);
}

int main() {
    int requests_per_department = 300000;
    char filename1[50], filename2[50];

    snprintf(filename1, sizeof(filename1), "load_department_%d.dat", 1);
    snprintf(filename2, sizeof(filename2), "load_department_%d.dat", 2);

    generate_load_file(1, requests_per_department, filename1);
    generate_load_file(2, requests_per_department, filename2);

    return 0;
}
