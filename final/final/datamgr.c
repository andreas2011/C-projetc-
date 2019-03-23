#include <inttypes.h>
#include <string.h>
#include "config.h"
#include "lib/dplist.h"
#include "datamgr.h"
#define LOG_MAX_LEN 1024
void write_fifo(const char* log_event);

typedef uint16_t room_id_t;
typedef uint16_t data_cnt_t;

static dplist_t *sensor_list = NULL;
/*
*  The structure for sensor node
*/
typedef struct{
    sensor_id_t sensor_id;//sensor id
    room_id_t room_id;// room id
    sensor_value_t running_data[RUN_AVG_LENGTH];// data to compute a running average
    data_cnt_t cnt;//
    sensor_ts_t timestamp;// a last - modified timestamp that contains the timestamp of the last received sensor data used
        //to update the running average of this sensor
} sensor_node_data_t;
// copy funtion for sensor node in dplist
void *data_element_copy(void *element)
{
    sensor_node_data_t *sensor = (sensor_node_data_t *)element;
    sensor_node_data_t *copy = malloc(sizeof(sensor_node_data_t));
    memcpy(copy->running_data, sensor->running_data, sizeof(sensor_value_t) * RUN_AVG_LENGTH);
    copy->cnt = sensor->cnt;
    copy->room_id = sensor->room_id;
    copy->sensor_id = sensor->sensor_id;
    copy->timestamp = sensor->timestamp;
    return (void *)copy;
}
// free function for sensor node in dplist
void data_element_free(void **element)
{
    free((*element));
    *element = NULL;
}
// compare funtion for sensor node in dplist
int data_element_compare(void *x, void *y)
{
    return (((sensor_node_data_t *)x)->sensor_id) != (((sensor_node_data_t *)y)->sensor_id);
}
/*
 *  This method holds the core functionality of your datamgr. It takes in 2 file pointers to the sensor files and parses them. 
 *  When the method finishes all data should be in the internal pointer list and all log messages should be printed to stderr.
 */
void datamgr_parse_sensor_files(FILE * fp_sensor_map, FILE * fp_sensor_data)
{
    unsigned int sensor_id, room_id;
    sensor_data_t sensor_data;
    sensor_node_data_t *psensor = NULL;
    sensor_node_data_t sensor;
    //create sensor node list
	sensor_list = dpl_create(data_element_copy, data_element_free, data_element_compare);
    ERROR_HANDLER(fp_sensor_map == NULL, "error");
    ERROR_HANDLER(fp_sensor_data == NULL, "error");
    // read data from sensor map
    while (fscanf(fp_sensor_map, "%04u %04u \n", &room_id, &sensor_id) == 2){
        //create sensor node and initialed
        psensor = malloc(sizeof(sensor_node_data_t));
        ERROR_HANDLER(psensor == NULL, "error");
        memset(psensor, 0, sizeof(sensor_node_data_t));
        psensor->room_id = room_id;
        psensor->sensor_id = sensor_id;
        psensor->cnt = 0;
        // insert sensor node into sensor node list
        dpl_insert_at_index(sensor_list, (void *)psensor, 0, false);
    }
    // read sensor data from sensor_data file
    while (fread(&sensor_data.id, sizeof(uint16_t), 1, fp_sensor_data)){
        fread(&sensor_data.value, sizeof(double), 1, fp_sensor_data);
        fread(&sensor_data.ts, sizeof(time_t), 1, fp_sensor_data);
        sensor.sensor_id = sensor_data.id;
        sensor.timestamp = sensor_data.ts;
        // find the sensor
        int idx = dpl_get_index_of_element(sensor_list, (void *)&sensor);
        if (idx == -1){
            printf("Sensor id %"PRIu16" did not occur in room_sensor.map\n", sensor_data.id);
        }
        else{
            // collecting sensor data
            psensor = dpl_get_element_at_index(sensor_list, idx);
            psensor->running_data[psensor->cnt % RUN_AVG_LENGTH] = sensor_data.value;
            psensor->cnt++;
            // computes for every sensor node a running average
            if (psensor->cnt >= RUN_AVG_LENGTH){
                sensor_value_t run_avg = 0;
                for (int i = 0; i < RUN_AVG_LENGTH; i++){
                    run_avg += psensor->running_data[i];
                }
                // too hot 
                if (run_avg / RUN_AVG_LENGTH > SET_MAX_TEMP){
                    fprintf(stderr,"room %"PRIu16" too hot.\n", psensor->room_id);
                }
                // too cold
                else if (run_avg / RUN_AVG_LENGTH < SET_MIN_TEMP){
                    fprintf(stderr,"room %"PRIu16" too cold.\n", psensor->room_id);
                }
            }
            psensor->timestamp = sensor_data.ts;
        }
    }
}


/*
* Reads continiously all data from the shared buffer data structure, parse the room_id's
* and calculate the running avarage for all sensor ids
* When *buffer becomes NULL the method finishes. This method will NOT automatically free all used memory
*/
void datamgr_parse_sensor_data(FILE * fp_sensor_map, sbuffer_t ** buffer1, sbuffer_t ** buffer2)
{
	unsigned int sensor_id, room_id;
	sensor_data_t sensor_data;
	sensor_node_data_t *psensor = NULL;
	sensor_node_data_t sensor;
	char log_buf[LOG_MAX_LEN];
	//create sensor node list
	sensor_list = dpl_create(data_element_copy, data_element_free, data_element_compare);
	ERROR_HANDLER(fp_sensor_map == NULL, "error");
	ERROR_HANDLER((*buffer1) == NULL, "error");
	ERROR_HANDLER((*buffer2) == NULL, "error");

	// read data from sensor map
	while (fscanf(fp_sensor_map, "%04u %04u \n", &room_id, &sensor_id) == 2){
		//create sensor node and initialed
		psensor = malloc(sizeof(sensor_node_data_t));
		ERROR_HANDLER(psensor == NULL, "error");
		memset(psensor, 0, sizeof(sensor_node_data_t));
		psensor->room_id = room_id;
		psensor->sensor_id = sensor_id;
		psensor->cnt = 0;
		// insert sensor node into sensor node list
		dpl_insert_at_index(sensor_list, (void *)psensor, 0, false);
	}
	// read sensor data from sensor_data file
	while (1){
		if (sbuffer_remove(*buffer1, &sensor_data) != SBUFFER_SUCCESS)
			break;
		
		sensor.sensor_id = sensor_data.id;
		sensor.timestamp = sensor_data.ts;
		// find the sensor
		int idx = dpl_get_index_of_element(sensor_list, (void *)&sensor);
		if (idx == -1){
			snprintf(log_buf, LOG_MAX_LEN, "Received sensor data with invalid sensor node ID %" PRIu16 ".\n", sensor_data.id);
			write_fifo(log_buf);
			//printf("Sensor id %"PRIu16" did not occur in room_sensor.map\n", sensor_data.id);
		}
		else{
			// collecting sensor data
			if (sbuffer_insert(*buffer2, &sensor_data) != SBUFFER_SUCCESS)
				break;

			psensor = dpl_get_element_at_index(sensor_list, idx);
			psensor->running_data[psensor->cnt % RUN_AVG_LENGTH] = sensor_data.value;
			psensor->cnt++;
			// computes for every sensor node a running average
			if (psensor->cnt >= RUN_AVG_LENGTH){
				sensor_value_t run_avg = 0;
				for (int i = 0; i < RUN_AVG_LENGTH; i++){
					run_avg += psensor->running_data[i];
				}
				run_avg /= RUN_AVG_LENGTH;
				// too hot 
				if (run_avg > SET_MAX_TEMP){
					snprintf(log_buf, LOG_MAX_LEN, 
						"The sensor node with %" PRIu16 " reports it's too hot (running avg temperature = %g).\n", 
						psensor->sensor_id, run_avg);
					write_fifo(log_buf);
					//fprintf(stderr, "room %"PRIu16" too hot.\n", psensor->room_id);
				}
				// too cold
				else if (run_avg < SET_MIN_TEMP){
					snprintf(log_buf, LOG_MAX_LEN,
						"The sensor node with %" PRIu16 " reports it's too cold (running avg temperature = %g).\n",
						psensor->sensor_id, run_avg);
					write_fifo(log_buf);
					//fprintf(stderr, "room %"PRIu16" too cold.\n", psensor->room_id);
				}
			}
			psensor->timestamp = sensor_data.ts;
		}
	}
}

/*
 * This method should be called to clean up the datamgr, and to free all used memory. 
 * After this, any call to datamgr_get_room_id, datamgr_get_avg, datamgr_get_last_modified or datamgr_get_total_sensors will not return a valid result
 */
void datamgr_free()
{
    dpl_free(&sensor_list, true);
    sensor_list = NULL;
}
    
/*   
 * Gets the room ID for a certain sensor ID
 * Use ERROR_HANDLER() if sensor_id is invalid 
 */
uint16_t datamgr_get_room_id(sensor_id_t sensor_id)
{
    sensor_node_data_t snode;
    sensor_node_data_t *p_snode;
    int idx;
    snode.sensor_id = sensor_id;
    idx = dpl_get_index_of_element(sensor_list, (void *)&snode);
    ERROR_HANDLER(idx == -1, "error");
    p_snode = dpl_get_element_at_index(sensor_list, idx);
    return p_snode->room_id;
}


/*
 * Gets the running AVG of a certain senor ID (if less then RUN_AVG_LENGTH measurements are recorded the avg is 0)
 * Use ERROR_HANDLER() if sensor_id is invalid 
 */
sensor_value_t datamgr_get_avg(sensor_id_t sensor_id)
{
    sensor_node_data_t snode;
    sensor_node_data_t *p_snode;
    sensor_value_t run_avg = 0;
    data_cnt_t cnt;
    snode.sensor_id = sensor_id;
    int idx = dpl_get_index_of_element(sensor_list, (void *)&snode);
    ERROR_HANDLER(idx == -1, "error");
    p_snode = dpl_get_element_at_index(sensor_list, idx);
    if (p_snode->cnt >= RUN_AVG_LENGTH)
        cnt = RUN_AVG_LENGTH;
    else
        cnt = p_snode->cnt;
    
    for (int i = 0; i < cnt; i++){
        run_avg += p_snode->running_data[i];
    }
    
    return run_avg / cnt;
}


/*
 * Returns the time of the last reading for a certain sensor ID
 * Use ERROR_HANDLER() if sensor_id is invalid 
 */
time_t datamgr_get_last_modified(sensor_id_t sensor_id)
{
    sensor_node_data_t snode;
    sensor_node_data_t *p_snode;
    int idx;
    snode.sensor_id = sensor_id;
    idx = dpl_get_index_of_element(sensor_list, (void *)&snode);
    ERROR_HANDLER(idx == -1, "error");

    p_snode = dpl_get_element_at_index(sensor_list, idx);
    return p_snode->timestamp;
}


/*
 *  Return the total amount of unique sensor ID's recorded by the datamgr
 */
int datamgr_get_total_sensors()
{
    return dpl_size(sensor_list);
}
   

