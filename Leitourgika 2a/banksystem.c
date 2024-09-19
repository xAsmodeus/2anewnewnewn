 #include "bank_system.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <asm-generic/socket.h> // for SO_REUSEPORT 

#define NUM_RECORDS 1000
#define DEPT1_RECORDS 100  
#define NUM_REQUESTS 50    
#define PORT 8080          

 pthread_mutex_t lock;      


void create_records(const char *filename)
{
    FILE *file = fopen(filename, "wb");
    if (!file)
    {
        perror("Failed to create file");
        exit(1);
    }

    srand(time(NULL));
    Record records[NUM_RECORDS];

    // Initialize records for department 1
    for (int i = 0; i < DEPT1_RECORDS; i++)
    {
        records[i].accountNumber = i + 1;
        records[i].amount = (float)(rand() % 1000) / 100;
        records[i].departmentNumber = 1;
    }

    // Initialize records for department 2
    for (int i = DEPT1_RECORDS; i < NUM_RECORDS; i++)
    {
        records[i].accountNumber = i + 1;
        records[i].departmentNumber = 2;
        records[i].amount = (float)(rand() % 10000) / 100;
    }

    fwrite(records, sizeof(Record), NUM_RECORDS, file);
    fclose(file);
}

float get_amount(int accountNumber, const char *filename)
{
    FILE *file = fopen(filename, "rb");
    if (!file)
    {
        perror("Failed to open file");
        exit(1);
    }

    Record record;
    while (fread(&record, sizeof(Record), 1, file))
    {
        if (record.accountNumber == accountNumber)
        {
            fclose(file);
            return record.amount;
        }
    }

    fclose(file);
    return -1.0; // Not found
}

void update_amount(int accountNumber, float amount, const char *filename)
{
    FILE *file = fopen(filename, "r+b");
    if (!file)
    {
        perror("Failed to open file");
        exit(1);
    }

    Record record;
    int found = 0;

    // Attempt to find and update the account
    while (fread(&record, sizeof(Record), 1, file))
    {
        if (record.accountNumber == accountNumber)
        {
            found = 1;
            break;
        }
    }

    if (found)
    {
       
         //the record
        fseek(file, -sizeof(Record), SEEK_CUR);
        fwrite(&record, sizeof(Record), 1, file);
        fflush(file); // Ensure the write is complete

        // Perform the update
        record.amount += amount;
        fseek(file, -sizeof(Record), SEEK_CUR);
        fwrite(&record, sizeof(Record), 1, file);
    }

    fclose(file);
}

int transfer_amount(FILE *file, int fromAccount, int toAccount, double amount) {
    Record record;
    int fromFound = 0, toFound = 0;
    double fromAmount = 0, toAmount = 0;

    pthread_mutex_lock(&lock); // Κλείδωμα για αποφυγή ταυτόχρονης πρόσβασης
    rewind(file); // Επιστροφή στην αρχή του αρχείου
    while (fread(&record, sizeof(Record), 1, file) == 1) {
        if (record.accountNumber == fromAccount) {
            if (record.amount >= amount) { // Έλεγχος αν υπάρχει επαρκές υπόλοιπο
                fromAmount = record.amount - amount; // Αφαίρεση του ποσού
                fromFound = 1;
            } else {
                pthread_mutex_unlock(&lock); // Ξεκλείδωμα πριν επιστρέψουμε
                return -1; // Μη επαρκές υπόλοιπο
            }
        } else if (record.accountNumber == toAccount) {
            toAmount = record.amount + amount; // Προσθήκη του ποσού
            toFound = 1;
        }
    }

    if (!fromFound || !toFound) {
        pthread_mutex_unlock(&lock); // Ξεκλείδωμα
        return -1; // Δεν βρέθηκαν οι λογαριασμοί
    }

    // Επιστροφή στην αρχή για την ενημέρωση
    rewind(file);
    while (fread(&record, sizeof(Record), 1, file) == 1) {
        if (record.accountNumber == fromAccount) {
            record.amount = fromAmount; // Ενημέρωση του "from" λογαριασμού
            fseek(file, -sizeof(Record), SEEK_CUR);
            fwrite(&record, sizeof(Record), 1, file);
        } else if (record.accountNumber == toAccount) {
            record.amount = toAmount; // Ενημέρωση του "to" λογαριασμού
            fseek(file, -sizeof(Record), SEEK_CUR);
            fwrite(&record, sizeof(Record), 1, file);
        }
    }

    fflush(file); // Καθαρισμός buffer εγγραφής
    pthread_mutex_unlock(&lock); // Ξεκλείδωμα

    return 0; // Επιτυχής μεταφορά
}
void create_load_files(int department_number, FILE *file)
{
    srand(time(NULL));
    Request requests[NUM_REQUESTS];

    for (int i = 0; i < NUM_REQUESTS; i++)
    {
        requests[i].departmentNumber = department_number;
        int randval = rand() % 100;

        if (randval < 35)
        {
            requests[i].queryType = 1;
            requests[i].queryDetails[0] = (rand() % NUM_RECORDS) + 1;
        }
        else if (randval < 70)
        {
            requests[i].queryType = 2;
            requests[i].queryDetails[0] = (rand() % NUM_RECORDS) + 1;
            requests[i].queryDetails[1] = (rand() % 2000) - 1000;
        }
        else if (randval < 95)
        {
            requests[i].queryType = 3;
            requests[i].queryDetails[0] = (rand() % NUM_RECORDS) + 1;
            requests[i].queryDetails[1] = (rand() % NUM_RECORDS) + 1;
            requests[i].queryDetails[2] = (rand() % 2000);
        }
        else
        {
            requests[i].queryType = 4;
            requests[i].queryDetails[0] = department_number;
        }
    }

    fwrite(requests, sizeof(Request), NUM_REQUESTS, file);
}

void print_records(Record records[], int num_records)
{
    for (int i = 0; i < num_records; i++)
    {
        printf("Account Number: %d, Department: %d, Amount: %.2f\n",
               records[i].accountNumber, records[i].departmentNumber, records[i].amount);
    }
}

void average_amount(unsigned char departmentNumber, const char *filename)
{
    FILE *file = fopen(filename, "rb");
    if (!file)
    {
        perror("Failed to open file");
        return;
    }

    float total = 0.0;
    int count = 0;
    Record record;

    // Read records from the file and accumulate the total amount for the specified department
    while (fread(&record, sizeof(Record), 1, file))
    {
        if (record.departmentNumber == departmentNumber)
        {
            total += record.amount;
            count++;
        }
    }

    fclose(file);

    if (count > 0)
    {
        float average = total / count;
        time_t now = time(NULL);
        printf("Average amount in department %d is %.2f at %s", departmentNumber, average, ctime(&now));
    }
    else
    {
        printf("No records found for department %d\n", departmentNumber);
    }
}

//central server functions
void process_request(const Request *request, const char *records_filename)
{
    
    FILE *file = fopen(records_filename, "r+b");
    if (!file)
    {
        perror("Failed to open file");
        return;
    }
 
    pthread_mutex_lock(&lock); // Lock for eager update

    switch (request->queryType)
    {
    case 1:
    {
        float amount = get_amount(request->queryDetails[0], records_filename);
        printf("amount for account %d: %.2f\n", request->queryDetails[0], amount);
        break;
    }
    case 2:
    {
        update_amount(request->queryDetails[0], request->queryDetails[1], records_filename);
        break;
    }
    case 3:
    {
        transfer_amount(file, request->queryDetails[1], request->queryDetails[2], (double)request->queryDetails[2]);
        break;
    }
    case 4:
    {
        average_amount(request->departmentNumber, records_filename);
        break;
    }
    default:
        printf("Unknown request type: %d\n", request->queryType);
    }

    pthread_mutex_unlock(&lock); // Unlock after update
}

void *serve_requests(void *arg)
{
    int server_fd, new_socket, valread;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    Request request;

    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Forcefully attaching socket to the port
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Bind socket
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming connections
    if (listen(server_fd, 3) < 0)
    {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    while (1)
    {
        // Accept connection
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0)
        {
            perror("Accept failed");
            exit(EXIT_FAILURE);
        }

        // Read request
        valread = read(new_socket, &request, sizeof(Request));
        if (valread < 0) {
            perror("Read failed");
            close(new_socket);
            continue;
        }
        // Process request
        process_request(&request, "global_records.txt");

        close(new_socket);
    }

    return NULL;
}
//local servers functions



void initialize_local_server()
{
    pthread_t thread_id;
    pthread_mutex_init(&lock, NULL);

    // Create thread to serve requests
    if (pthread_create(&thread_id, NULL, serve_requests, NULL) != 0)
    {
        perror("Thread creation failed");
        exit(EXIT_FAILURE);
    }
}