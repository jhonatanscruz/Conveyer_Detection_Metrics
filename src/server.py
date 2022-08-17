#!/usr/bin/env python3
# UDP SERVER to receive messages sending by CAPICOM Laser RF627SMART SERIES
# Created April 2022
# Last Update: July 29th, 2022
# By Jhonatan Cruz from Fttech Software Team

import credentials, socket, threading, time, struct, serial, psycopg2, math
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client

"""
#################################################################
#                                                               #
#               Functions to decode received data               #
#                                                               #
#################################################################
"""
def getFloat(bytestring, start):
    payload = b''

    for c in range(start, start+4, 1):
        payload += bytestring[c].to_bytes(1,'big')

    return struct.unpack('<f', payload)[0]

def getInt(bytestring, start):
    payload = b''

    for c in range(start, start+4, 1):
        payload += bytestring[c].to_bytes(1,'big')

    return int.from_bytes(payload, byteorder='little')

"""
#################################################################
#                                                               #
#              Functions to manipulate Postgres DB              #
#                                                               #
#################################################################
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
#         Functions to send information to Amazon Alexa         #
#                                                               #
#################################################################
"""
def sendMQTT(msg, my_id = credentials.alexa_id, username = credentials.alexa_user, password = credentials.alexa_pass, broker = credentials.alexa_broker, port = credentials.alexa_port, topic = credentials.alexa_topic):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(my_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    client.publish(topic, msg)
    client.disconnect()

def getState(r_align, l_align):

    centerPosition = (r_align + l_align)/20

    if centerPosition <= -1:
        return 0

    elif (centerPosition > -1) and (centerPosition <= -0.5):
        return 1

    elif (centerPosition > -0.5) and (centerPosition <= 0.5):
        return 2

    elif (centerPosition > 0.5) and (centerPosition <= 1):
        return 3

    else:
        return 4

def conveyorState(r_align, l_align, stateVector, currentState):

    currState = currentState

    if len(stateVector) < 3:
        stateVector.append(getState(r_align, l_align))

        if len(stateVector) == 3:
            if stateVector[0] == stateVector[1] and stateVector[1] == stateVector[2]:
                if currState is not stateVector[0]:
                    currState = stateVector[0]
                    stateVector.clear()
                    if currState == 0:
                        msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À ESQUERDA"
                    elif currState == 1:
                        msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À ESQUERDA"
                    elif currState == 2:
                        msg = "TUDO CERTO! A ESTEIRA ESTÁ NOVAMENTE CENTRALIZADA"
                    elif currState == 3:
                        msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À DIREITA"
                    elif currState == 4:
                        msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À DIREITA"

                    try:
                        sendMQTT(msg)
                    except:
                        pass

    else:
        stateVector[0] = stateVector[1]
        stateVector[1] = stateVector[2]
        stateVector[2] = getState(r_align, l_align)

        if stateVector[0] == stateVector[1] and stateVector[1] == stateVector[2]:
            if currState is not stateVector[0]:
                currState = stateVector[0]
                stateVector.clear()
                if currState == 0:
                    msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À ESQUERDA"
                elif currState == 1:
                    msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À ESQUERDA"
                elif currState == 2:
                    msg = "TUDO CERTO! A ESTEIRA ESTÁ NOVAMENTE CENTRALIZADA"
                elif currState == 3:
                    msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À DIREITA"
                elif currState == 4:
                    msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À DIREITA"

                try:
                    sendMQTT(msg)
                except:
                    pass
              
    return currState

"""
#################################################################
#                                                               #
#                       General Functions                       #
#                                                               #
#################################################################
"""

def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def InitalSetup(sock, buffer):
    data = []
    read = sock.recvfrom(buffer)

    area = getFloat(read[0],0)
    right_align = getFloat(read[0],4)
    left_align = getFloat(read[0],12)
    profile = getInt(read[0],20)
    pulse = getInt(read[0],24)
    time = now()

    data.append(area)
    data.append(right_align)
    data.append(left_align)
    data.append(profile)
    data.append(pulse)
    data.append(time)

    return data

def calcVolume(last_area, current_area, last_pulse, current_pulse):
    mm2_to_m2 = 0.000001 # Converts mm2 to m2
    pulse_to_mm = 0.05   # Converts each pulse to value in milimeters
    mm_to_m = 0.001    # Converts mm to m
    volume_aprox = 0.94  # Approximation from volume calculated to real value

    delta_pulse = current_pulse - last_pulse
    mean_area = ((current_area + last_area)/2) * mm2_to_m2 # m²
    volume = (mean_area * delta_pulse * pulse_to_mm * mm_to_m) * volume_aprox # Volume in m³

    return volume

def calcVelocity(delta_pulse, delta_time):
    pulse_to_mm = 0.05   # Converts each pulse to value in milimeters
    mm_to_m = 0.001    # Converts mm to m

    mean_pulse = sum(delta_pulse)/len(delta_pulse)
    mean_time = sum(delta_time)/len(delta_time)

    velocity = (mean_pulse * pulse_to_mm * mm_to_m) / mean_time # m/s

    return velocity

"""
##############################################
#                                            #
#                    MAIN                    #
#                                            #
##############################################
"""
if __name__ == "__main__":

    ##################################
    #                                #
    #        GLOBAL VARIABLES        #
    #                                #
    ##################################
    debug_0 = 0 # Print Debug
    debug_1 = 0 # TXT Debug
    send_to_DB = 3 #seconds
    volume_accumulated = 0
    pulse_accumulated = []
    delta_time_accumulated = []
    velocity = 0
    last_profile_count = 0
    last_pulse_count = 0
    last_time_received = 0
    last_area_received = 0

    ############################################
    #                                          #
    #        UDP CONNECTION CREDENTIALS        #
    #                                          #
    ############################################
    HOST    = credentials.udp_host
    PORT    = credentials.udp_port
    address = credentials.udp_adress
    buffer  = credentials.udp_buffer
    conn    = credentials.udp_conn

    ###########################################
    #                                         #
    #        DB CONNECTION CREDENTIALS        #
    #                                         #
    ###########################################
    _host = credentials.postgres_host
    _port = credentials.postgres_port
    _db   = credentials.postgres_db
    _user = credentials.postgres_user
    _pass = credentials.postgres_pass

    ##################################
    #                                #
    #          ALEXA ALERTS          #
    #                                #
    ##################################
    stateVector = []
    currentState = 0
    states = ["extremely left-aligned", "slightly left-aligned", "center aligned", "slightly right-aligned", "extremely right-aligned"]

    # ----------------- Wait for Ethernet connection ----------------- #
    time.sleep(20) # wait 15 seconds before initialize

    #######################################################
    #                                                     #
    #                 START TABLE METRICS                 #
    #                                                     #
    #######################################################

    db = startDB(_host, _port, _db, _user, _pass)
    try:
        sql = 'SELECT id FROM metrics ORDER BY ID DESC LIMIT 1'
        db.cursor().execute(sql)
        closeDB(db)

    except:
        closeDB(db)
        db = startDB(_host, _port, _db, _user, _pass)
        sql = 'create table metrics(id SERIAL primary key, volume REAL, velocity REAL, right_align REAL, left_align REAL, timestamp TIMESTAMP WITHOUT TIME ZONE);'
        createInsertDB(db, sql)
        closeDB(db)

    #######################################################
    #                                                     #
    #                    UDP CONNECTION                   #
    #                                                     #
    #######################################################
    while conn == False:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(address)
            conn = True
        except:
            time.sleep(1)
            pass

    # --------------- Aguarda mensagens ---------------
    if debug_0:
        print ('Aguardando mensagens...')

    # ------------- Initial Required Setup -------------
    setup = InitalSetup(sock, buffer)
    last_area_received = setup[0]
    currentState = getState(setup[1], setup[2])
    last_profile_count = setup[3]
    last_pulse_count = setup[4]
    last_time_received = setup[5]

    if currentState == 0:
        msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À ESQUERDA"
    elif currentState == 1:
        msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À ESQUERDA"
    elif currentState == 2:
        msg = "A ESTEIRA ESTÁ CENTRALIZADA"
    elif currentState == 3:
        msg = "ALERTA! ESTEIRA DESALINHADA: UM POUCO À DIREITA"
    elif currentState == 4:
        msg = "ALERTA! ESTEIRA DESALINHADA: MUITO À DIREITA"

    try:
        sendMQTT(msg)
    except:
        pass

    init = now()

    while True:
        # ------------- Leitura das mensagens -------------
        conn = False
        while conn == False:
            try:
                data = sock.recvfrom(buffer)
                current_time = now()
                conn = True
            except:
                pass

        area = getFloat(data[0],0)
        dist = getFloat(data[0],28)
        if (dist >= 46) and (dist <= 54):
            right_align = getFloat(data[0],4)
            left_align = getFloat(data[0],12)
        pulse_count = getInt(data[0],24)
        client_address = data[1]

        ##### Metrics #####
        if pulse_count != last_pulse_count:
            # Volume Calculation and accumulate
            volume_accumulated = volume_accumulated + calcVolume(last_area_received, area, last_pulse_count, pulse_count)

            # Accumulate values in vectors of pulse and time to calculate mean velocity
            pulse_accumulated.append(pulse_count - last_pulse_count)
            delta_time_accumulated.append((current_time - last_time_received).total_seconds())

            ##### REFRESH VALUES #####
            last_area_received = area
            last_pulse_count = pulse_count
            last_time_received = current_time

            # Verify if time to send data to DB has been reached
            diff = current_time - init
            if (diff.total_seconds() >= send_to_DB):
                # Evaluates changes in conveyor position then sends alert to ALEXA SPEAKER
                currState = conveyorState(right_align, left_align, stateVector, currentState)
                currentState = currState

                # Velocity Calculation
                velocity = calcVelocity(pulse_accumulated, delta_time_accumulated)

                # Converts from mm to cm
                right_align = right_align/10
                left_align = left_align/10

                ##### SEND TO DB #####
                db = startDB(_host, _port, _db, _user, _pass)
                sql = f"insert into metrics values (default, '{volume_accumulated}','{velocity}', '{right_align}', '{left_align}', '{now()}')"
                createInsertDB(db, sql)
                closeDB(db)

                ##### REFRESH VALUES #####
                init = now()
                pulse_accumulated = []
                delta_time_accumulated = []
                volume_accumulated = 0

            if debug_0:
                print(f"Client: {client_address} | Area: {area} | r_align: {right_align} | l_align: {left_align} | Pulse: {pulse_count}")

            if debug_1:
                # ------------- Stores data to data.log file -------------
                if area!='' or distancia!='':
                    #abre o arquivo de log
                    arq = open("data.log", "a")
                    # Escreve a payload
                    arq.write(str(data[0]) + "\n")
                    # Escreve a hora
                    arq.write('[' + str(now()) + ']')
                    # Escreve os dados
                    arq.write(f" Client: {client_address} | Area: {area} | r_align: {right_align} | l_align: {left_align} | Pulse: {pulse_count}\n")
                    arq.close()
