#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dplist.h"

/*
 * definition of error codes
 * */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

#ifdef DEBUG
	#define DEBUG_PRINTF(...) 									         \
		do {											         \
			fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__);	 \
			fprintf(stderr,__VA_ARGS__);								 \
			fflush(stderr);                                                                          \
                } while(0)
#else
	#define DEBUG_PRINTF(...) (void)0
#endif


#define DPLIST_ERR_HANDLER(condition,err_code)\
	do {						            \
            if ((condition)) DEBUG_PRINTF(#condition " failed\n");    \
            assert(!(condition));                                    \
        } while(0)

        
/*
 * The real definition of struct list / struct node
 */

struct dplist_node {
  dplist_node_t * prev, * next;
  void * element;
};

struct dplist {
  dplist_node_t * head;
  void * (*element_copy)(void * src_element);			  
  void (*element_free)(void ** element);
  int (*element_compare)(void * x, void * y);
};


dplist_t * dpl_create (// callback functions
			  void * (*element_copy)(void * src_element),
			  void (*element_free)(void ** element),
			  int (*element_compare)(void * x, void * y)
			  )
{
  dplist_t * list;
  list = malloc(sizeof(struct dplist));
  DPLIST_ERR_HANDLER(list==NULL,DPLIST_MEMORY_ERROR);
  list->head = NULL;  
  list->element_copy = element_copy;
  list->element_free = element_free;
  list->element_compare = element_compare; 
  return list;
}

void dpl_free(dplist_t ** list, bool free_element)
{
        DPLIST_ERR_HANDLER(*list==NULL,DPLIST_INVALID_ERROR);
        dplist_node_t *head = (*list)->head, *tmp = NULL;
        while (head)
        {
          tmp = head->next;
           //whether free the element or not
            if (free_element)
            {
            (*list)->element_free(&(head->element));
            }
         //free the current node
         free(head);
         head = tmp;
         }
       //free the lis
        free(*list);
       //set the pointer to null
        (*list) = NULL;
    
    
}

dplist_t * dpl_insert_at_index(dplist_t * list, void * element, int index, bool insert_copy)
{
    
    // add your code here
    dplist_node_t *ref_node = NULL, *list_node = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    //creat a new node
    list_node = malloc(sizeof(dplist_node_t));
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_MEMORY_ERROR);
    list_node->element = NULL;
    list_node->next = NULL;
    list_node->prev = NULL;
    //whether deep copy or not
    if (insert_copy){
        list_node->element = list->element_copy(element);
    }
    else{
        list_node->element = element;
    }
    //if list null set head to new node
    if (list->head == NULL){
        list->head = list_node;
    }
    else{
        //index<=0
        if (index <= 0){
            list_node->next = list->head;
            list->head->prev = list_node;
            list->head = list_node;
        }
        else{
            
            ref_node = dpl_get_reference_at_index(list, index);
            assert(ref_node != NULL);
            
            if (index < dpl_size(list)){
                list_node->prev = ref_node->prev;
                list_node->next = ref_node;
                ref_node->prev->next = list_node;
                ref_node->prev = list_node;
            }
            else{
                
                assert(ref_node->next == NULL);
                list_node->prev = ref_node;
                ref_node->next = list_node;
            }
            
        }
    }
    return list;
}

dplist_t * dpl_remove_at_index( dplist_t * list, int index, bool free_element)
{
    
    // add your code here
    dplist_node_t *ref_node = NULL;
    int size;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    //get the size of the list
    size = dpl_size(list);
    
    if (size > 0)
        {
        
        ref_node = dpl_get_reference_at_index(list, index);
        assert(ref_node != NULL);
        
        if (size == 1){
            list->head = NULL;
        }
        else
        {
            
            if (index <= 0){
                list->head = ref_node->next;
            }else if (size - 1 <= index){
                ref_node->prev->next = NULL;
            }
            else{
                ref_node->prev->next = ref_node->next;
                ref_node->next->prev = ref_node->prev;
                }
            
        }
        
    if (free_element)
        {
            list->element_free(&(ref_node->element));
        }
        free(ref_node);
        ref_node=NULL;
    }
    return list;
}


int dpl_size( dplist_t * list )
{
    
    int size = 0;
    dplist_node_t *dummy = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    
    
    for (dummy = list->head; dummy!= NULL; dummy = dummy->next)
    {
        size++;
    }
    return size;
}

dplist_node_t * dpl_get_reference_at_index( dplist_t * list, int index )
{
    
    int cnt=0;
    dplist_node_t * dummy;
	
    DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
    
        if(list->head==NULL)
	return NULL;
	
        if(index<=0)
	return list->head;
	
	
        for(dummy = list->head;dummy!=NULL;dummy=dummy->next,cnt++)
	{
		if(cnt >= index)
		{
			return dummy;
		}
		if(dummy->next == NULL)
		{
			return dummy;
		}

	}
	return NULL;
}

void * dpl_get_element_at_index( dplist_t * list, int index )
{
    

    int cnt = 0;
    dplist_node_t * head = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);

    if (list->head == NULL)
        return (void*)0;
    
    for (head = list->head; head->next != NULL; head = head->next, cnt++){
        if (cnt >= index)
            return head->element;
    }
    return head->element;
}

int dpl_get_index_of_element( dplist_t * list, void * element )
{
    
     
    int idx = 0;
    dplist_node_t * head = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    
    if (element == NULL)
        return -1;
    
    for (head = list->head; head != NULL; head = head->next, idx++){
        if (list->element_compare(head->element, element) == 0)
            return idx;
    }
    
    return -1;
}

// HERE STARTS THE EXTRA SET OF OPERATORS //

// ---- list navigation operators ----//
  
dplist_node_t * dpl_get_first_reference( dplist_t * list )
{
    
    DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
    if(list->head == NULL)
        return NULL;
    else
        return list->head;
}

dplist_node_t * dpl_get_last_reference( dplist_t * list )
{
    
    dplist_node_t * head = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    
    if (list->head == NULL)
        return NULL;
    
    for (head = list->head; head->next != NULL; head = head->next);
    return head;
}

dplist_node_t * dpl_get_next_reference( dplist_t * list, dplist_node_t * reference )
{
    
    dplist_node_t *dummy;
    DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
    
    if(list->head == NULL)
	{
        return NULL;
	}
    
    if(reference == NULL)
	{
        return NULL;
	}
    
    for(dummy = list->head;dummy->next != NULL; dummy = dummy->next)
    {
        if(dummy == reference)
            return dummy->next;
    }
	
    if(dummy == reference)
	{
        return dummy->next;
	}
    
    return NULL;
    
    
}

dplist_node_t * dpl_get_previous_reference( dplist_t * list, dplist_node_t * reference )
{
    
    dplist_node_t *dummy;
    DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
    
    if(list->head == NULL)
        return NULL;

    
    if(reference == NULL)
        return NULL;

    
    for(dummy = list->head;dummy->next != NULL; dummy = dummy->next)
    {
        if(dummy == reference)
		{
            return dummy->prev;
		}
    }
    if(dummy == reference)
        return dummy->prev;
    
    return NULL;
}

// ---- search & find operators ----//  
  
void * dpl_get_element_at_reference( dplist_t * list, dplist_node_t * reference )
{
    
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    
    if (list->head == NULL)
        return NULL;
    
    if (reference == NULL){
        dplist_node_t *last = dpl_get_last_reference(list);
        assert(last != NULL);
        return last->element;
    }

    
    if (dpl_get_index_of_reference(list, reference) == -1)
        return NULL;
    
    return reference->element;
	
}

dplist_node_t * dpl_get_reference_of_element( dplist_t * list, void * element )
{
   
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
   
    if (list->head == NULL)
        return NULL;
    
    int idx = dpl_get_index_of_element(list, element);
    
    if (idx == -1)
        return NULL;
 
    return dpl_get_reference_at_index(list, idx);
}

int dpl_get_index_of_reference( dplist_t * list, dplist_node_t * reference )
{
     
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
    
    if (list->head == NULL)
        return -1;
    
    if (reference == NULL){
       
        int size = dpl_size(list);
        
        return size - 1;
    }
    
    dplist_node_t *head = list->head;
    int idx = 0;
    while (head != NULL){
        if (reference == head){
            return idx;
        }
        head = head->next;
        idx++;
    }
    return -1;
    
}
  
// ---- extra insert & remove operators ----//

dplist_t * dpl_insert_at_reference( dplist_t * list, void * element, dplist_node_t * reference, bool insert_copy )
{
    
    
    DPLIST_ERR_HANDLER(list==NULL, DPLIST_INVALID_ERROR);
    
    if(reference == NULL)
    {
        return dpl_insert_at_index(list, element, dpl_size(list), insert_copy);
    }
    else
    {
		
        int index = dpl_get_index_of_reference(list, reference);
        
        if(index == -1)
        return list;
       
        else
		{
            return dpl_insert_at_index(list, element, index, insert_copy);
		}
    }
}

dplist_t * dpl_insert_sorted( dplist_t * list, void * element, bool insert_copy )
{
    
    
   
    dplist_node_t *head = NULL, *list_node = NULL;
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
   
    list_node = malloc(sizeof(dplist_node_t));
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_MEMORY_ERROR);
    list_node->element = NULL;
    list_node->next = NULL;
    list_node->prev = NULL;
   
    if (insert_copy){
        list_node->element = list->element_copy(element);
    }
    else{
        list_node->element = element;
    }
  
    if (list->head == NULL){
        list->head = list_node;
    }
    else{
       
        dplist_node_t *first = dpl_get_first_reference(list);
        dplist_node_t *last = dpl_get_last_reference(list);
        assert(last != NULL && first != NULL);
       
        if (list->element_compare(list_node->element, first->element) <= 0){
            list->head = list_node;
            list_node->next = first;
            first->prev = list_node;
        }else if (list->element_compare(list_node->element, last->element) > 0){
       
            last->next = list_node;
            list_node->prev = last;
        }
        else{
            
            head = list->head;
            while (head != NULL){
                if (list->element_compare(list_node->element, head->element) <= 0){
					list_node->prev = head->prev;
					list_node->next = head;
					head->prev->next = list_node;
					head->prev = list_node;
					break;
				}
                head = head->next;
            }
            
        }
    }
    return list;
    
}

dplist_t * dpl_remove_at_reference( dplist_t * list, dplist_node_t * reference, bool free_element )
{
    
    int index = dpl_get_index_of_reference(list,reference);
    DPLIST_ERR_HANDLER(list==NULL, DPLIST_INVALID_ERROR);
    
    if(reference == NULL)
	{
		dpl_remove_at_index(list,dpl_size(list),free_element);
		return list;
	}
	
	
	if(index == -1)
	  return list;
	
	dpl_remove_at_index(list,index,free_element);
	return list;
    
}

dplist_t * dpl_remove_element( dplist_t * list, void * element, bool free_element )
{
    
   
    DPLIST_ERR_HANDLER(list==NULL, DPLIST_INVALID_ERROR);
    int index = dpl_get_index_of_element(list, element);
    
	if(list->head == NULL) 
	return list;
	
    
    
    
    if(index == -1)
        return list;
    else
        return dpl_remove_at_index(list, index, free_element);

    
}
  
// ---- you can add your extra operators here ----//



