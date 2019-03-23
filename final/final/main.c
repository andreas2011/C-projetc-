#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include "connmgr.h"
#include "datamgr.h"
#include "sbuffer.h"
#include "sensor_db.h"

#define LOG_MAX_LEN 1024
void gateway_help(void);
void create_fifo(void);
void log_write_process(void);
void write_fifo(const char* log_event);
void *connmgr_start(void *arg);
void *datamgr_start(void *arg);
void *stgmgr_start(void *arg);

int is_gateway_close();
void gateway_closed();

const char* fifo_name = "logFifo";
const char* log_file_name = "gateway.log";
const char* room_map = "room_sensor.map";
const char* terminated_msg = "Sensor gateway terminated...\n";
FILE *fifo_write_fd = NULL;


sbuffer_t *connmgr_to_datamgr, *datamgr_to_stgmgr;

int gateway_run = 1;
pthread_mutex_t gateway_mutex;
int main(int argc, char *argv[])
{
	if (argc != 2){
		gateway_help();
		exit(EXIT_FAILURE);
	}
	printf("Main process %d is running...\n", getpid());
	int port = atoi(argv[1]);
	pid_t log_pid;
	log_pid = fork();
	if (log_pid < 0){
		printf("fork log process failure!\n");
		exit(1);
	} else if(log_pid == 0){
		log_write_process();
	}

	create_fifo();
	fifo_write_fd = fopen(fifo_name, "w");
	if (fifo_write_fd == NULL){
		perror("Open fifo error\n");
		exit(1);
	}

	if (sbuffer_init(&connmgr_to_datamgr) != SBUFFER_SUCCESS ||
		sbuffer_init(&datamgr_to_stgmgr) != SBUFFER_SUCCESS){
		write_fifo("Create share buffer failure!\n");
		fclose(fifo_write_fd);
		gateway_run = 0;
		exit(EXIT_FAILURE);
	}

	pthread_t connmgr_tid, datamgr_tid, stgmgr_tid;
	pthread_create(&connmgr_tid, NULL, &connmgr_start, &port);
	pthread_create(&datamgr_tid, NULL, &datamgr_start, NULL);
	pthread_create(&stgmgr_tid, NULL, &stgmgr_start, NULL);
	pthread_join(connmgr_tid, NULL);
	pthread_join(datamgr_tid, NULL);
	pthread_join(stgmgr_tid, NULL);
	
	if (sbuffer_free(&connmgr_to_datamgr) != SBUFFER_SUCCESS ||
		sbuffer_free(&datamgr_to_stgmgr) != SBUFFER_SUCCESS){
		write_fifo("Free share buffer failure!\n");
	}
	
	write_fifo(terminated_msg);
	log_pid = wait(NULL);
	fclose(fifo_write_fd);
	printf("Main process %d terminated...\n", getpid());
	return 0;
}

void gateway_help(void)
{
	printf("Use this program with 1 command line options: \n");
	printf("\t%-15s : TCP server port number\n", "\'server port\'");
}

void create_fifo()
{
	int res = -1;
	if (access(fifo_name, F_OK) < 0){
		res = mkfifo(fifo_name, 0777);
		if (res < 0){
			perror("Create FIFO error\n");
			exit(1);
		}
	}
}

void log_write_process()
{
	int sqe_num = 0;
	char read_buf[LOG_MAX_LEN];
	FILE *log_fd, *fifo_read_fd;
	printf("Log process %d is running...\n", getpid());

	
	create_fifo();
	fifo_read_fd = fopen(fifo_name, "r");
	if (fifo_read_fd == NULL){
		perror("Open fifo error\n");
		exit(EXIT_FAILURE);
	}

	log_fd = fopen(log_file_name, "w");
	if (fifo_read_fd == NULL){
		perror("Open log file error\n");
		fclose(fifo_read_fd);
		exit(EXIT_FAILURE);
	}
	while (1){
		if (fgets(read_buf, LOG_MAX_LEN, fifo_read_fd) != NULL){
			fprintf(log_fd, "%d %ld %s", sqe_num, time(NULL), read_buf);
			//fprintf(stdout, "%d %s", sqe_num, read_buf);
			sqe_num++;
		}
		if (strcmp(read_buf, terminated_msg) == 0)
			break;
	}
	fprintf(log_fd, "%d %ld Log process terminated...\n", sqe_num++, time(NULL));
	fclose(fifo_read_fd);
	fclose(log_fd);
	printf("Log process %d terminated...\n", getpid());
	exit(EXIT_SUCCESS);
}

void write_fifo(const char* log_event)
{
	char write_buf[LOG_MAX_LEN];
	memcpy(write_buf, log_event, LOG_MAX_LEN);
	//fprintf(stdout, "%s", write_buf);
	if (fputs(write_buf, fifo_write_fd) == EOF){
		perror("Write fifo error");
	}
	fflush(fifo_write_fd);

}

void *connmgr_start(void *arg)
{
	int *server_port = (int *)arg;
	write_fifo("connection manager run...\n");
	connmgr_listen(*server_port, connmgr_to_datamgr);
	connmgr_free();
	write_fifo("connection manager terminated...\n");
	gateway_closed();
	pthread_exit(NULL);
}

void *datamgr_start(void *arg)
{
	FILE *room_fd = fopen(room_map, "r");
	write_fifo("data manager run...\n");
	if (room_fd == NULL){
		perror("Open room_sensor.map file error");
		gateway_run = 0;
		pthread_exit(NULL);
	}
	datamgr_parse_sensor_data(room_fd, &connmgr_to_datamgr, &datamgr_to_stgmgr);
	datamgr_free();
	fclose(room_fd);
	write_fifo("data manager terminated...\n");
	gateway_closed();
	pthread_exit(NULL);
}

void *stgmgr_start(void *arg)
{
	DBCONN *conn = NULL;
	int attempts = 3;
	write_fifo("storage manager run...\n");
	for (int i = 0; i < attempts; i++){
		conn = init_connection(1);
		if (conn != NULL)
			break;
		else
			sleep(3);
	}

	storagemgr_parse_sensor_data(conn, &datamgr_to_stgmgr);
	disconnect(conn);
	write_fifo("storage manager terminated...\n");
	gateway_closed();
	pthread_exit(NULL);
}

int is_gateway_close()
{
	int ret;
	pthread_mutex_lock(&gateway_mutex);
	ret = !gateway_run;
	pthread_mutex_unlock(&gateway_mutex);
	return ret;

}
void gateway_closed()
{
	pthread_mutex_lock(&gateway_mutex);
	gateway_run = 0;
	pthread_mutex_unlock(&gateway_mutex);
	
}
