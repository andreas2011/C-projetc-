#ifndef CONNMGR_H
#define CONNMGR_H

#define MAX_CONN 1024
#include "sbuffer.h"

#ifndef TIMEOUT
    #error TIMEOUT not set
#endif
/*
 * This method holds the core functionality of your connmgr. 
 * It starts listening on the given port and when when a sensor node connects it writes the data to a sensor_data_recv file.
 * This file must have the same format as the sensor_data file in assignment 6 and 7.
 */
void connmgr_listen(int port_number, sbuffer_t *write_buf);

/*
 * This method should be called to clean up the connmgr, 
 * and to free all used memory. After this no new connections will be accepted.
 */
void connmgr_free();

/*
 * Also this connection manager should be using your dplist to store all the info on the active sensor nodes.
 */
#endif /* CONNMGR_H */