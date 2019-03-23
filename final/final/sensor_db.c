 #include <stdio.h>
#include <stdlib.h>
#include "config.h"
#include <sqlite3.h>
#include <inttypes.h>
#include <unistd.h>
#include "sensor_db.h"
#define LOG_MAX_LEN 1024
void write_fifo(const char* log_event);

//judge if table exist
int table_exist(void *ret, int argc, char **argv, char **az_col_name)
{
    if (argc == 1){
        int *table_cnt = (int *)ret;
        *table_cnt = 1;
    }
    return 0;
}

/*
* Reads continiously all data from the shared buffer data structure and stores this into the database
* When *buffer becomes NULL the method finishes. This method will NOT automatically disconnect from the db
*/
void storagemgr_parse_sensor_data(DBCONN * conn, sbuffer_t ** buffer)
{
	char log_buf[LOG_MAX_LEN];
	int attempts = 3, res;
	if ((*buffer) == NULL)
		return;
	while (1){
		sensor_data_t data;
		if (sbuffer_remove(*buffer, &data) != SBUFFER_SUCCESS)
			break;
		for (int i = 0; i < attempts; i++){
			res = insert_sensor(conn, data.id, data.value, data.ts);
			if (!res){
				//snprintf(log_buf, LOG_MAX_LEN, "[%" PRIu16 ", %lf, %ld]\n", data.id, data.value, data.ts);
				//write_fifo(log_buf);
				break;
			}
			else{
				snprintf(log_buf, LOG_MAX_LEN, "Connection to SQL server lost, try attempt times %d\n", i);
				write_fifo(log_buf);
				sleep(3);
			}
		}
		if (res){
			
			break;
		}
	}
}
/*
 * Make a connection to the database server
 * Create (open) a database with name DB_NAME having 1 table named TABLE_NAME  
 * If the table existed, clear up the existing data if clear_up_flag is set to 1
 * Return the connection for success, NULL if an error occurs
 */

DBCONN * init_connection(char clear_up_flag)
{
    sqlite3 *db = NULL;
    char *err_msg = NULL;
    int table_cnt = 0;
	char log_buf[LOG_MAX_LEN];
    char *sql_exsit = sqlite3_mprintf("SELECT name FROM sqlite_master where type = 'table' and name = '%q'", TO_STRING(TABLE_NAME));
    //open database
    int ret = sqlite3_open(TO_STRING(DB_NAME), &db);
    if (ret != SQLITE_OK){
		snprintf(log_buf, LOG_MAX_LEN, "Unable to connect to SQL server : %s\n", sqlite3_errmsg(db));
		write_fifo(log_buf);
        //printf("Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        sqlite3_free(sql_exsit);
        return NULL;
    }
	snprintf(log_buf, LOG_MAX_LEN, "%s\n","Connection to SQL server established.");
	write_fifo(log_buf);
    //query if table exsit
    ret = sqlite3_exec(db, sql_exsit, table_exist, &table_cnt, &err_msg);
    if (ret != SQLITE_OK){
		snprintf(log_buf, LOG_MAX_LEN, "SQL error: %s\n", err_msg);
		write_fifo(log_buf);
        //printf("SQL error1: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        sqlite3_free(sql_exsit);
        return NULL;
    }
    if (table_cnt == 0){
        // if table do not exsit, create it
        char *sql_create = "CREATE TABLE "TO_STRING(TABLE_NAME)"(id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id INT, sensor_value DECIMAL(4,2), timestamp TIMESTAMP);";
        ret = sqlite3_exec(db, sql_create, 0, 0, &err_msg);
        if (ret != SQLITE_OK){
			snprintf(log_buf, LOG_MAX_LEN, "New table %s created failure : %s\n", TO_STRING(TABLE_NAME), err_msg);
			write_fifo(log_buf);
            //printf("SQL error2: %s\n", err_msg);
            sqlite3_free(err_msg);
            sqlite3_close(db);
            sqlite3_free(sql_exsit);
            return NULL;
        }
		snprintf(log_buf, LOG_MAX_LEN, "New table %s created.\n", TO_STRING(TABLE_NAME));
		write_fifo(log_buf);
    }
    else{
        if (clear_up_flag == 1){
            // if table exsit, delete all data
            char *sql_del = "DELETE FROM "TO_STRING(TABLE_NAME)";";
            ret = sqlite3_exec(db, sql_del, &table_exist, &table_cnt, &err_msg);
            if (ret != SQLITE_OK){
				snprintf(log_buf, LOG_MAX_LEN, "Cleanup table %s failure : %s\n", TO_STRING(TABLE_NAME), err_msg);
				write_fifo(log_buf);
                //printf("SQL error3: %s\n", err_msg);
                sqlite3_free(err_msg);
                sqlite3_close(db);
                sqlite3_free(sql_exsit);
                return NULL;
            }
        }
    }
    sqlite3_free(sql_exsit);
    return db;

}


/*
 * Disconnect from the database server
 */
void disconnect(DBCONN *conn)
{
    // close database
    int ret = sqlite3_close(conn);
    if (ret == SQLITE_BUSY){
        printf("SQL error: close failure\n");
    }
}


/*
 * Write an INSERT query to insert a single sensor measurement
 * Return zero for success, and non-zero if an error occurs
 */
int insert_sensor(DBCONN * conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts)
{
    char *err_msg = NULL;
	
    //initial sql sentence
    char *sql_insert = sqlite3_mprintf("INSERT INTO %s(sensor_id, sensor_value, timestamp) VALUES (%u, %lf, %ld)", TO_STRING(TABLE_NAME),
        id, value, ts);
    // insert sensor data to database
    int ret = sqlite3_exec(conn, sql_insert, 0, 0, &err_msg);
    if (ret != SQLITE_OK){
		char log_buf[LOG_MAX_LEN];
		snprintf(log_buf, LOG_MAX_LEN, "SQL error: %s\n", err_msg);
		write_fifo(log_buf);
        //printf("SQL error4: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_insert);
        return 1;
    }
    sqlite3_free(sql_insert);
    return 0;
}


/*
 * Write an INSERT query to insert all sensor measurements available in the file 'sensor_data'
 * Return zero for success, and non-zero if an error occurs
 */
int insert_sensor_from_file(DBCONN * conn, FILE * sensor_data)
{
    sensor_id_t id;
    sensor_value_t value;
    sensor_ts_t ts;
    // read data from sensor data file 
    while (fread(&id, sizeof(uint16_t), 1, sensor_data)){
        fread(&value, sizeof(double), 1, sensor_data);
        fread(&ts, sizeof(time_t), 1, sensor_data);
        //insert sensor data into database
        if (insert_sensor(conn, id, value, ts) != 0){
            printf("Insert sensor data error: %u, %lf, %ld\n", id, value, ts);
            return 1;
        }
    }
    return 0;
}


/*
  * Write a SELECT query to select all sensor measurements in the table 
  * The callback function is applied to every row in the result
  * Return zero for success, and non-zero if an error occurs
  */
int find_sensor_all(DBCONN * conn, callback_t f)
{
    char *err_msg = NULL;
    //initial select sql
    char *sql_select = sqlite3_mprintf("SELECT * FROM %q", TO_STRING(TABLE_NAME));
    // query all sensor data
    int ret = sqlite3_exec(conn, sql_select, f, 0, &err_msg);
    if (ret != SQLITE_OK){
        printf("SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_select);
        return 1;
    }
    sqlite3_free(sql_select);
    return 0;
}


/*
 * Write a SELECT query to return all sensor measurements having a temperature of 'value'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */
int find_sensor_by_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
    char *err_msg = NULL;
    //initial select sql
    char *sql_select = sqlite3_mprintf("SELECT * FROM %q where sensor_value = %lf", TO_STRING(TABLE_NAME), value);
    // query sensor by value
    int ret = sqlite3_exec(conn, sql_select, f, 0, &err_msg);
    if (ret != SQLITE_OK){
        printf("SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_select);
        return 1;
    }
    sqlite3_free(sql_select);
    return 0;

}


/*
 * Write a SELECT query to return all sensor measurements of which the temperature exceeds 'value'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */
int find_sensor_exceed_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
    char *err_msg = NULL;
    //initial select sql
    char *sql_select = sqlite3_mprintf("SELECT * FROM %q where sensor_value > %lf", TO_STRING(TABLE_NAME), value);
    // query sensor data 
    int ret = sqlite3_exec(conn, sql_select, f, 0, &err_msg);
    if (ret != SQLITE_OK){
        printf("SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_select);
        return 1;
    }
    sqlite3_free(sql_select);
    return 0;
}


/*
 * Write a SELECT query to return all sensor measurements having a timestamp 'ts'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */
int find_sensor_by_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
    char *err_msg = NULL;
    //initial select sql
    char *sql_select = sqlite3_mprintf("SELECT * FROM %q where timestamp = %ld", TO_STRING(TABLE_NAME), ts);
    // query sensor data
    int ret = sqlite3_exec(conn, sql_select, f, 0, &err_msg);
    if (ret != SQLITE_OK){
        printf("SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_select);
        return 1;
    }
    sqlite3_free(sql_select);
    return 0;
}


/*
 * Write a SELECT query to return all sensor measurements recorded after timestamp 'ts'
 * The callback function is applied to every row in the result
 * return zero for success, and non-zero if an error occurs
 */
int find_sensor_after_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
    char *err_msg = NULL;
    //initial select sql
    char *sql_select = sqlite3_mprintf("SELECT * FROM %q where timestamp > %ld", TO_STRING(TABLE_NAME), ts);
    // query sensor data
    int ret = sqlite3_exec(conn, sql_select, f, 0, &err_msg);
    if (ret != SQLITE_OK){
        printf("SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql_select);
        return 1;
    }
    sqlite3_free(sql_select);
    return 0;
}


