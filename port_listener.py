'''
    This module is tasked with listening to the serial port and reporting that data.
'''
# Python import
import socket
import selectors
import time
from datetime import datetime
import copy
import threading

# Custom python imports
from threading_python_api.threadWrapper import threadWrapper # pylint: disable=import-error
import system_constants # pylint: disable=import-error

#import DTO for communicating internally
from logging_system_display_python_api.DTOs.logger_dto import logger_dto # pylint: disable=import-error
from logging_system_display_python_api.DTOs.print_message_dto import print_message_dto # pylint: disable=import-error
from logging_system_display_python_api.DTOs.byte_report import byte_report_dto # pylint: disable=import-error

class port_listener(threadWrapper):
    '''
        This module opens the serial port and listens to it. 
    '''
    def __init__(self, coms, thread_name:str = "", batch_size:int= 1024, port:str="", host:str="", batch_collection_number_before_save:int = 10):
        self.__function_dict = {
            'run' : self.run,
            'connect' : self.connect,
            'get_connected' : self.get_connected,
            'get_status_web' : self.get_status_web,
            'create_tap' : self.create_tap,
        }
        super().__init__(self.__function_dict)
        self.__status_lock = threading.Lock()
        self.__tap_requests_lock = threading.Lock()
        self.__coms = coms
        self.__connected = False
        self.__byte_count_received = 0
        self.__connections_count = 0
        self.__thread_name = thread_name
        self.__server_address = (host, port)
        self.__batch_collection_number_before_save = batch_collection_number_before_save
        self.__last_received = time.time()

        if 16 <= batch_size <= 1024: 
            self.__batch_size = batch_size
        else : 
            raise Exception(f'The given batch size {batch_size} is not with in bounds max allowed 1024 min allowed 16.') # pylint: disable=w0719
        
        self.__buffer_idx = 0
        self.__data_dict = {
            'batch_sample': [bytearray(b'\x00' * self.__batch_size) for _ in range(self.__batch_collection_number_before_save)]
        }
        self.__data_dict_idx = 0

        self.__client_socket = None
        self.__selectors = None
        self.__have_received = False
        
        try:
            self.connect()
        except : # pylint: disable=w0702
            self.__connected = False
        self.__tap_requests = []
        self.__subscriber = []
        new_table = {
            self.__thread_name : [['batch_sample', self.__batch_size, 'byte']],
        }

        self.__coms.send_request(system_constants.database_name, ['create_table_external', new_table])
    def run(self): #pylint: disable=R0915 #pylint disable=R1702
        '''
            This function actually listens to the tcpip port, and sends that data away to be saved. 
        '''
        super().set_status('Running')
        start = time.time()
        

        # print(f"Started port listener at {self.__server_address[0]}:{self.__server_address[1]}")

        while super().get_running(): #pylint: disable=R1702
            #check to see if there is another task to be done
            request = super().get_next_request()
            # check to see if there is a request
            if request is not None:
                if len(request[1]) > 0:
                    request[3] = self.__function_dict[request[0]](request[1])
                else : 
                    request[3] = self.__function_dict[request[0]]()
                super().complete_request(request[4], request[3])
            
            #Now lets handel data coming in 
            if self.__connected:
                try :
                    ### check for data ###
                    events = self.__selectors.select(timeout=0.1)  # Wait for events with a timeout of 0.1 seconds
                    if not events: # No events, sleep for a short duration
                        time.sleep(0.1)  # Adjust the sleep duration as needed
                    else : # we have an event lets handle it
                        for key, mask in events:
                            callback = key.data
                            callback(key, mask)

                    ### Reporting ###
                    if time.time() - start > 1: #check to see if it has been one second
                        report = byte_report_dto(self.__thread_name, str(datetime.now()), self.__byte_count_received)
                        # Report the how many bytes we have received
                        self.__coms.report_bytes(report)
                        self.__byte_count_received = 0
                        start = time.time() # set start to the new starting point

                    ### Save Data if too much time has passed ###
                    if (time.time() - self.__last_received) > 5 and self.__have_received: #if it has been 5 seconds since we have seen data save what we have to the data base and if batch_sample has been crated, and we have data to save. 
                        data_dict_copy = copy.deepcopy(self.__data_dict)
                        self.send_data(data_dict_copy=data_dict_copy)
                        # clear the buffer
                        for i in range(self.__batch_collection_number_before_save):
                            for j in range(self.__batch_size):
                                self.__data_dict['batch_sample'][i][j] = 0
                        self.__have_received = False
                        self.__data_dict_idx = 0      
                                           
                except Exception as e: # pylint: disable=w0718
                    print(f"Serial listener had an error: {e}")
                    self.close_connection()
                    dto = print_message_dto(f'Failed to open serial port for listening at {self.__server_address}, will retry in 5 seconds. \n'+ f'{self.__thread_name} ->' + 'Error: ' + str(e))
                    self.__coms.print_message(dto, typeM=1)
                    print(f'Failed to open serial port for listening at {self.__server_address}, will retry in 5 seconds. \n'+ f'{self.__thread_name} ->' + 'Error: ' + str(e))
                    time.sleep(5) #wait for some time
                    self.connect()#try and connect
            else :
                time.sleep(0.5) #wait for some time
                self.connect()#try and connect
                if self.__connected : 
                    start = time.time()
        ### if the users ends the program then we close things off here
        self.close_connection()
    def data_received_call_back(self, key, _):
        '''
            This function handles receiving data when we get the event 
        '''
        client_socket = key.fileobj
        ### Collect data from the port ###
        bytes_received = client_socket.recv(self.__batch_size)
        
        self.__byte_count_received += len(bytes_received)

        if self.__byte_count_received > 0 :
            self.__have_received = True
            self.__last_received = time.time()
        else :
            self.__have_received = False


        for byte in bytes_received:
            self.__data_dict['batch_sample'][self.__data_dict_idx][self.__buffer_idx] = byte
            self.__buffer_idx += 1

            if self.__buffer_idx == self.__batch_size:
                ### Save data if we have received a full batch ###
                self.__buffer_idx = 0
                self.__data_dict_idx += 1
                if self.__data_dict_idx == self.__batch_collection_number_before_save: # if we are full save the data. 
                    data_dict_copy = copy.deepcopy(self.__data_dict)
                    self.send_data(data_dict_copy=data_dict_copy)
                    # clear the buffer
                    for i in range(self.__batch_collection_number_before_save):
                        for j in range(self.__batch_size):
                            self.__data_dict['batch_sample'][i][j] = 0
                    
                    self.__data_dict_idx = 0
    def send_data(self, data_dict_copy):
        '''
            This function sends the data out to everyone who wants a copy of it. 
        '''
        try :
            self.__coms.send_request(system_constants.database_name, ['save_byte_data', self.__thread_name, data_dict_copy, self.__thread_name])
            if self.__tap_requests_lock.acquire(timeout=10): # pylint: disable=R1732
                for tap in self.__tap_requests:
                    tap(data_dict_copy['batch_sample'], self.__thread_name)
                self.__tap_requests_lock.release()
            else :
                raise RuntimeError(f"Port listener {self.__thread_name} could not acquire tap requests lock")
            
        except Exception as e: #pylint: disable=w0718
            print(e)
            self.__tap_requests_lock.release()
    def connect(self):
        '''
            This function connects to the serial port. 
        '''
        dto = print_message_dto(f'Connecting to port at {self.__server_address}, will retry in 5 seconds')
        self.__coms.print_message(dto, typeM=1)
        try :
            # Create a client socket
            self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Connect to the server
            self.__client_socket.connect(self.__server_address)
            
            #set up selectors so we can implement a callback function
            self.__selectors = selectors.DefaultSelector()

            self.__selectors.register(self.__client_socket, selectors.EVENT_READ, data=self.data_received_call_back)


            self.__connected = True
            if self.__connections_count < 1:
                dto = logger_dto(message=f"Serial port connected for listing at {self.__server_address}", time=str(datetime.now()))
                self.__coms.send_message_permanent(dto)
            self.__connections_count += 1
        except Exception as e: # pylint: disable=w0718
            self.__connected = False
            dto = print_message_dto('Could not connect ' + str(e))
            self.__coms.print_message(dto, typeM=1)
            self.__coms.report_additional_status(self.__thread_name, " Not Connected!")
            print(f"Error in connecting to the port serial reader {e}")
    def close_connection(self):
        '''
            This function closes the serial port.

            ARGS:
                None 
        '''
        self.__client_socket.close()
        self.__connected = False
        self.__coms.report_additional_status(self.__thread_name, " Not Connected!")  
    def get_connected(self):
        '''
            Returns the status of where it is connect to the serial port. 
        '''
        return self.__connected
    def create_tap(self, args):
        '''
            This function creates a tap, a tap will send the data it receives from the serial line to the class that created the tap.
            ARGS:
                args[0] : tap function to call. 
                args[1] :  name of subscriber
        '''
        if self.__tap_requests_lock.acquire(timeout=10): # pylint: disable=R1732
            self.__tap_requests.append(args[0])
            self.__subscriber.append(args[1])
            self.__tap_requests_lock.release()
        else :
            raise RuntimeError(f"Port listener {self.__thread_name} could not acquire tap requests lock")
    def get_status_web(self):
        '''
            This function returns the status for the serial listener to the webpage. 
        '''
        if self.__status_lock.acquire(timeout=10): # pylint: disable=R1732
            temp = {
                'port' : self.__thread_name,
                'connected' : self.__connected,
                'buad_rate' : "NA",
                'stopbits' : "NA",
                'subscribers' : str(self.__subscriber) if len(self.__subscriber) >= 1 else "No Subscribers",
            }
            self.__status_lock.release()
        else :
            raise RuntimeError("Could not aquire status lock")
        return temp
    def kill_Task(self):
        '''
            Small modification here to make sure the serial port gets closed. 
        '''
        self.__ser.close()
        threadWrapper.kill_Task()
