import sys
sys.path.append("../../../RF62X-Wrappers/Python/")
from PYSDK_SMART import *

if __name__ == '__main__':

    # Initialize sdk library
    sdk_init()

    # Create value for scanners vector's type
    list_scanners=search()

    # Iterate over all available network adapters in the current operating
    # system to send "Hello" requests.
    i=0
    for scanner in list_scanners: 
        info = get_info(scanner, kSERVICE)
        i+=1

        # Establish connection to the RF627 device by Service Protocol.
        is_connected = connect(scanner)
        if (not is_connected):
            print("Não foi possível estabelecer conexão com o scanner, favor verificar se está ligado!")
            continue

        # Get profile from scanner's data stream by Service Protocol.
        zero_points=True
        realtime=True
        count=0
        profile = get_profile2D(scanner,zero_points,realtime,kSERVICE)

        if (is_connected and profile is not None):
            profile_data_type=profile['header']['data_type']
            if profile_data_type == PROFILE_DATA_TYPES.PROFILE:
                numb = profile['points_count']

            if 'points' in profile:
                buf = "conveyor.txt"
                arq = open(buf, "a")
                for j in range(numb):
                    msg = str(profile['points'][j].z) + "\n"
                    arq.write(msg)

            #if 'intensity' in profile:
            #    for j in range(numb):
            #        print('-intensity [', j, ']\t', '=', profile['intensity'][j])
            print("Calibração Finalizada... ")
            print("-----------------------------------------")
        else:
            print("Houve um problema na calibração, favor verificar se o laser está ativo!")
            print("-----------------------------------------")

        # Disconnect from scanner.
        disconnect(scanner)

    # Cleanup resources allocated with sdk_init()
    sdk_cleanup()
