"""
#######################################################
#                                                     #
#   Generates 3D Surfaces reading 2D Profiles         #
#   from RF627 SMART Sensor                           #
#                                                     #
#   Autor: Jhonatan Da Silva Cruz                     #
#   Date: 2022-06-21                                  #
#   Last-Update: 2022-08-14                           #
#                                                     #
#######################################################
"""

#######################################################
#                                                     #
#                      LIBRARIES                      #
#                                                     #
#######################################################
import credentials, sys, time, psycopg2, matplotlib.pyplot as plt, numpy as np
from datetime import datetime, timezone
from PYSDK_SMART import *
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
from scipy import interpolate

#######################################################
#                                                     #
#                  GLOBAL FUNCTIONS                   #
#                                                     #
#######################################################
"""
#################################################################################
#                                                                               #
#   Calculates the mean height of a conveyor reading the "conveyor.txt" file    #
#   The .txt file can be generated using the "Conveyor_Calibrating.py" program	#
#   The mean height of conveyor is returned as a float value                    #
#                                                                               #
#################################################################################
"""
def getConveyorHeight():
    my_file = open("conveyor.txt", "r")

    content_list = my_file.readlines()
    h_conveyor_sum = 0
    count = 0

    for line in content_list:
        content_z = float(line)

        if (content_z == 0):
            pass

        else:
            h_conveyor_sum = h_conveyor_sum + content_z
            count = count + 1

    h_conveyor_mean = h_conveyor_sum/count

    return h_conveyor_mean

"""
#################################################################################
#                                                                               #
#                      Functions to manipulate Postgres DB                      #
#                                                                               #
#################################################################################
"""
def startDB(_host, _port, _db, _user, _pass):
    db = psycopg2.connect(host=_host, port=_port, database=_db, user=_user, password=_pass)
    return db

def createInsertDB(db, sql):
    cur = db.cursor()
    cur.execute(sql)
    db.commit()

def getFromDB(db, sql):
    cur = db.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    return recset

def closeDB(db):
    db.close()

"""
#################################################################
#                                                               #
#                       General Functions                       #
#                                                               #
#################################################################
"""

def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

#######################################################
#                                                     #
#                        MAIN                         #
#                                                     #
#######################################################
if __name__ == '__main__':

    # DATABASE CONNECTION CREDENTIALS
    _host= credentials.postgres_host
    _port= credentials.postgres_port
    _db=   credentials.postgres_db
    _user= credentials.postgres_user
    _pass= credentials.postgres_pass

    init = now()

    while True:
        end = now()
        diff = end - init
        if diff.total_seconds() >= 10:
            #################################################
            #                                               #
            #                     SETUP                     #
            #                                               #
            #################################################

            ############### Main Variables ################
            zero_points=True
            realtime=True
            profiles_numb = 0                   # Number of profiles received
            points_numb = 0                     # Number of points in each profile
            h_conveyor = getConveyorHeight()    # Conveyor Height to calibrate measures
            last_lecture_x = 0                  # Last x coordinate stored
            last_lecture_z = 0                  # Last z coordinate stored
            y = 0                               # Initial y coordinate value
            last_pulse_count = 0                # Last Pulse count received from the last profile
            delta_y = 0                         # Difference between 2 profiles in milimeters
            flag_0 = False                      # Returns True when a non zero height value is found
            X = []	                            # Matrice X for 3D Surface
            Y = []	                            # Matrice Y for 3D Surface
            Z = []	                            # Matrice Z for 3D Surface
            dump = []                           # Stores profile readings
            timestamp = []                      # Stores timestamp
            id = 1                              # Last ID stored in Metrics Database
            pulse_to_mm = 0.05                  # Converts each pulse to value in milimeters
            mm_to_cm = 0.1                      # Converts mm to m
            total_time_reading = 5              # Total time reading profiles from sensor
            delta_time_reading = 0              # Delta time reading profiles from sensor

            #######################################################
            #                                                     #
            #                  START TABLE PLOT                   #
            #                                                     #
            #######################################################
            db = startDB(_host, _port, _db, _user, _pass)
            try:
                sql = 'SELECT id FROM plot ORDER BY ID DESC LIMIT 1'
                recset = getFromDB(db, sql)
                if len(recset) != 0:
                    id = recset[0][0] + 1
                closeDB(db)

            except:
                closeDB(db)
                db = startDB(_host, _port, _db, _user, _pass)
                sql = 'create table plot(id SERIAL primary key, standard_view VARCHAR(100), front_view VARCHAR(100), side_view VARCHAR(100));'
                createInsertDB(db, sql)
                closeDB(db)

            ############# Initialize sdk library ############
            sdk_init()

            ######### Get scanner from scanners list ########
            list_scanners=search()

            #################################################
            #                                               #
            #              SENSOR INFORMATION               #
            #                                               #
            #################################################

            #info = get_info(scanner, kSERVICE)

            #################################################
            #                                               #
            #           VERIFY SENSOR CONNECTION            #
            #                                               #
            #################################################
            for scanner in list_scanners:

                ##### Establish connection to the RF627 device by Service Protocol #####
                is_connected = connect(scanner)
                if (not is_connected):
                    print("Failed to connect to scanner!")
                    continue

        	    #################################################
        	    #                                               #
        	    #           GET PROFILES FROM SENSOR            #
        	    #                                               #
        	    #################################################
                init_time_read = now()
                ###### Get profile from scanner's data stream by Service Protocol ######

                while delta_time_reading <= total_time_reading: # GETTING PROFILES FROM SENSOR
                    profile = get_profile2D(scanner,zero_points,realtime,kSERVICE)
                    if (profile is not None):
                        profiles_numb = profiles_numb + 1
                        dump.append(profile) # Get 2D Profile from Sensor
                    # Calculating delta time
                    diff = now() - init_time_read
                    delta_time_reading = diff.total_seconds()

        	    #################################################
        	    #                                               #
        	    #             PROFILES PROCESSING               #
        	    #                                               #
        	    #################################################
                last_pulse_count = dump[0]['header']['step_count']
                points_numb = dump[0]['points_count']

                for profile in dump:
                    # SETUP FOR EACH PROFILE
                    current_pulse_count = profile['header']['step_count']

                    # Calculating distance between 2 profiles
                    delta_y = (current_pulse_count - last_pulse_count) * pulse_to_mm * mm_to_cm

                    # Calculating distance between 2 profiles
                    y = y + delta_y

                    ##### REFRESH VALUES #####
                    last_pulse_count = current_pulse_count

                    if 'points' in profile:
                        for j in range(points_numb):
                            content_x = float(profile['points'][j].x)
                            content_z = float(profile['points'][j].z)

                            if ((content_x == 0 and content_z== 0)):
                                if(flag_0 == False):
                                    X.append(content_x)
                                    Z.append(content_z)

                                else: # Flag 0 is True, it means there is a non NULL height value stored in last_lecture variables
                                    X.append(last_lecture_x)
                                    Z.append(last_lecture_z)

                            else:
                                last_lecture_x = content_x/10
                                last_lecture_z = (content_z - h_conveyor)/10

                                if last_lecture_z < 0.21:
                                    last_lecture_z = 0

                                X.append(last_lecture_x)
                                Z.append(last_lecture_z)

                                if(flag_0 == False):
                                    flag_0 = True

                                    for i in range(j-1, -1, -1):
                                        X[i] = last_lecture_x
                                        Z[i] = last_lecture_z

                            Y.append(y)

                #######################################################
                #                                                     #
                #              RESHAPING VECTORS TO PLOT              #
                #                                                     #
                #######################################################
                X = np.reshape(X,(profiles_numb, points_numb))
                Y = np.reshape(Y,(profiles_numb, points_numb))
                Z = np.reshape(Z,(profiles_numb, points_numb))
                
                #tck = interpolate.bisplrep(X, Y, Z, s=0)
                #Znew = interpolate.bisplev(X[:,0], Y[0,:], tck)
                
                #######################################################
                #                                                     #
                #               SAVING STANDARD.PNG IMAGE             #
                #                                                     #
                #######################################################
                fig1 = plt.figure(figsize=(16, 9))#figsize=(4,4))
                ax1 = fig1.add_subplot(111, projection='3d')
                #ax.plot_wireframe(X, Y, Z,cmap=cm.hot)
                surf = ax1.plot_surface(X, Y, Z, cmap=cm.hot, linewidth=1, antialiased=True)
                ax1.set_xlim(-10, 10)
                #ax1.set_ylim(100, 125)
                ax1.set_zlim(0, 10)
                ax1.view_init(30,-60)
                #plt.axis('off')
                save_path = f"{credentials.device_path}standard{id}.png"
                db_path = f"{credentials.grafana_path}standard{id}.png"
                plt.savefig(save_path, dpi=300, bbox_inches="tight", pad_inches=0)
                standard_img = '<img width="100%c" src="%s">' % ('%', db_path)
                plt.close(fig1)

                #######################################################
                #                                                     #
                #                 SAVING SIDE.PNG IMAGE               #
                #                                                     #
                #######################################################
                fig1 = plt.figure(figsize=(16, 9))#figsize=(4,4))
                ax1 = fig1.add_subplot(111, projection='3d')
                #ax.plot_wireframe(X, Y, Z,cmap=cm.hot)
                surf = ax1.plot_surface(X, Y, Z, cmap=cm.hot, linewidth=1, antialiased=True)
                ax1.set_xlim(-10, 10)
                #ax1.set_ylim(100, 125)
                ax1.set_zlim(0, 10)
                ax1.view_init(30, 60)
                #plt.axis('off')
                save_path = f"{credentials.device_path}side{id}.png"
                db_path = f"{credentials.grafana_path}side{id}.png"
                plt.savefig(save_path, dpi=300, bbox_inches="tight", pad_inches=0)
                side_img = '<img width="100%c" src="%s">' % ('%', db_path)
                plt.close(fig1)

                #######################################################
                #                                                     #
                #               SAVING FRONT.PNG IMAGE                #
                #                                                     #
                #######################################################
                fig1 = plt.figure(figsize=(16, 9))#figsize=(4,4))
                ax1 = fig1.add_subplot(111, projection='3d')
                #ax.plot_wireframe(X, Y, Z,cmap=cm.hot)
                surf = ax1.plot_surface(X, Y, Z, cmap=cm.hot, linewidth=1, antialiased=True)
                ax1.set_xlim(-10, 10)
                #ax1.set_ylim(100, 125)
                ax1.set_zlim(0, 10)
                #plt.axis('off')
                ax1.view_init(0, 0)
                save_path = f"{credentials.device_path}front{id}.png"
                db_path = f"{credentials.grafana_path}front{id}.png"
                plt.savefig(save_path, dpi=300, bbox_inches="tight", pad_inches=0)
                front_img = '<img width="100%c" src="%s">' % ('%', db_path)
                plt.close(fig1)

                #######################################################
                #                                                     #
                #                 INSERT INTO PLOT DB                 #
                #                                                     #
                #######################################################
                db = startDB(_host, _port, _db, _user, _pass)
                sql = f"insert into plot values (default, '{standard_img}', '{front_img}', '{side_img}')"
                createInsertDB(db, sql)
                closeDB(db)

                #################################################
                #                                               #
                #           DISCONNECT SENSOR AND SDK           #
                #                                               #
                #################################################

                ######### Disconnect from scanner #########
                disconnect(scanner)

            ######### Cleanup resources allocated with sdk_init() #########
            sdk_cleanup()

            ##### REFRESH VALUES #####
            init = now()
