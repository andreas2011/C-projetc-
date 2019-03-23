#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <assert.h>
#include <inttypes.h>

#include "config.h"
#include "lib/tcpsock.h"
#include "lib/dplist.h"
#include "connmgr.h"
#define LOG_MAX_LEN 1024
int is_gateway_close();
void write_fifo(const char* log_event);

tcpsock_t *connmgr = NULL;
static dplist_t *sensor_list = NULL;


typedef struct{
    sensor_id_t sensor_id;
    tcpsock_t* conn;
    sensor_ts_t timestamp;
}sensor_node_t;
// copy funtion for sensor node in dplist
void *conn_element_copy(void *element)
{
    sensor_node_t *sensor = (sensor_node_t *)element;
    sensor_node_t *copy = malloc(sizeof(sensor_node_t));
    copy->conn = sensor->conn;
    copy->timestamp = sensor->timestamp;
    return (void *)copy;
}

// free function for sensor node in dplist
void conn_element_free(void **element)
{
    sensor_node_t *node = (sensor_node_t *)(*element);
    if (node->conn)
        tcp_close(&node->conn);
    free((*element));
    *element = NULL;
}
// compare funtion for sensor node in dplist
int conn_element_compare(void *x, void *y)
{
    tcpsock_t *xconn = ((sensor_node_t *)x)->conn;
    tcpsock_t *yconn = ((sensor_node_t *)y)->conn;
    int xfd, yfd;
    tcp_get_sd(xconn, &xfd);
    tcp_get_sd(yconn, &yfd);
    return xfd != yfd;
}

//get sensor node from sensor dplist by sock fd
sensor_node_t *get_sensor_node_by_fd(int sock_fd)
{
    int size = dpl_size(sensor_list);
    int sfd = -1;
    for (int i = 0; i < size; i++){
        sensor_node_t *node = dpl_get_element_at_index(sensor_list, i);
        if (node != NULL && tcp_get_sd(node->conn, &sfd) == TCP_NO_ERROR){
            if (sfd == sock_fd)
                return node;
        }
    }
    return NULL;
}

/*
* This method holds the core functionality of your connmgr.
* It starts listening on the given port and when when a sensor node connects it writes the data to a sensor_data_recv file.
* This file must have the same format as the sensor_data file in assignment 6 and 7.
*/
void connmgr_listen(int port_number, sbuffer_t *write_buf)
{
    int sock_fd;
    int ready_fds, cur_fds;
    time_t cur_time, last_time;
    struct pollfd sensor_fd[MAX_CONN];
    // create sensor dplist
	sensor_list = dpl_create(conn_element_copy, conn_element_free, conn_element_compare);

    //Creates a new socket and opens this socket in 'passive listening mode' (waiting for an active connection setup request)
    if (tcp_passive_open(&connmgr, port_number) != TCP_NO_ERROR){
		return;
    }
    //get listen sock fd
    tcp_get_sd(connmgr, &sock_fd);
    sensor_fd[0].fd = sock_fd;
    sensor_fd[0].events = POLLIN;
    cur_fds = 1;
    //initial other sock fd
    for (int i = 1; i < MAX_CONN; i++){
        sensor_fd[i].fd = -1;
    }
    time(&last_time);
	while (!is_gateway_close()){
		char log_buf[LOG_MAX_LEN];
        // use poll to check if there are sensor fds which ready to receive data per 10ms 
        ready_fds = poll(sensor_fd, cur_fds, 10);
        // if there are sensor fds which ready to receive data
        if (ready_fds > 0){
            // check if there are sensor node connect to connmgr
            if (sensor_fd[0].revents & POLLIN){
                tcpsock_t *sock;
                //A newly created socket identifying the remote system that initiated the connection request is returned
                if (tcp_wait_for_connection(connmgr, &sock) != TCP_NO_ERROR)
                    exit(EXIT_FAILURE);
                
                tcp_get_sd(sock, &sock_fd);
                // set sensor fd in free position
                for (int i = 1; i < MAX_CONN; i++){
                    if (sensor_fd[i].fd < 0){
                        sensor_fd[i].fd = sock_fd;
                        sensor_fd[i].events = POLLIN;
                        if (cur_fds <= i)
                            cur_fds++;
                        break;
                    }
                }
                // initial dplist node in insert
                sensor_node_t snode;
                snode.sensor_id = 0;
                snode.conn = sock;
                time(&snode.timestamp);
                dpl_insert_sorted(sensor_list, &snode, 1);
                ready_fds--;
            }
            if (!ready_fds)
                continue;

            for (int i = 1; i < cur_fds; i++){
                if (sensor_fd[i].fd < 0)
                    continue;
                // if sensor fd is valid
                sensor_node_t *node = get_sensor_node_by_fd(sensor_fd[i].fd);
                assert(node != NULL);
                tcpsock_t *sensor_sock = node->conn;
                // if sensor fd is ready to receive data
                if (sensor_fd[i].revents & POLLIN){
                    int bytes, result;
                    sensor_data_t data;
                    // receive data
                    bytes = sizeof(data.id);
                    result = tcp_receive(sensor_sock, (void*)&data.id, &bytes);
                    bytes = sizeof(data.value);
                    result = tcp_receive(sensor_sock, (void*)&data.value, &bytes);
                    bytes = sizeof(data.ts);
                    result = tcp_receive(sensor_sock, (void*)&data.ts, &bytes);
                    //receive data success
                    if ((result == TCP_NO_ERROR) && bytes){
						sbuffer_insert(write_buf, &data);
                        // update timestamp
						if (node->sensor_id == 0){
							snprintf(log_buf, LOG_MAX_LEN, "A sensor node with %" PRIu16 " has opened a new connection.\n", data.id);
							write_fifo(log_buf);
						}
                        node->timestamp = data.ts;
                        node->sensor_id = data.id;
                        //printf("sensor id = %" PRIu16 " - temperature = %g - timestamp = %ld\n", data.id, data.value, (long int)data.ts);
                    }else if (result == TCP_CONNECTION_CLOSED){
                        // if sensor node closed
						snprintf(log_buf, LOG_MAX_LEN, "The sensor node with %" PRIu16 " has closed the connection.\n", node->sensor_id);
						write_fifo(log_buf);
                        //printf("sensor node %d is closed\n", node->sensor_id);
                        sensor_fd[i].fd = -1;
                        dpl_remove_element(sensor_list, node, 1);
                        
                        if (i >= cur_fds - 1)
                            cur_fds--;
                    }

                }
                
            }
        }
        // check if sensor node connection is time out
        for (int i = 1; i < cur_fds; i++){
            if (sensor_fd[i].fd < 0)
                continue;
            sensor_node_t *node = get_sensor_node_by_fd(sensor_fd[i].fd);
            assert(node != NULL);
            
            time(&cur_time);
            // get difftime to judge if sensor node connection timeout
            if (difftime(cur_time, node->timestamp) > TIMEOUT){
                //printf("sensor node %d is timeout\n", node->sensor_id);
				snprintf(log_buf, LOG_MAX_LEN, "The sensor node with %" PRIu16 " has closed the connection.\n", node->sensor_id);
				write_fifo(log_buf);
                sensor_fd[i].fd = -1;
                dpl_remove_element(sensor_list, node, 1);
                if (i >= cur_fds - 1)
                    cur_fds--;
            }

        }

        time(&cur_time);
        // check if connmgr timeout
        if (dpl_size(sensor_list) == 0){
            if (difftime(cur_time, last_time) > TIMEOUT){
                //printf("connection manager timeout\n");
				snprintf(log_buf, LOG_MAX_LEN, "connection manager timeout\n");
				write_fifo(log_buf);
                break;
            }
                
        }
        else{
            last_time = cur_time;
        }

    }

}

/*
* This method should be called to clean up the connmgr,
* and to free all used memory. After this no new connections will be accepted.
*/
void connmgr_free()
{
    dpl_free(&sensor_list, 1);
	if (tcp_close(&connmgr) != TCP_NO_ERROR)
		return;
}