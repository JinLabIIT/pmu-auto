#!/usr/bin/env python3

import socket
import datetime
import math
import struct
import pandas as pd
import sys
import time
import argparse
import json
sys.path.append('../')
from utilities.pmu_csv_parser import parse_csv_data


# python3 send.py pmu12.csv --ip 10.0.2.2 -- port 4712 --num_packets 100 --drop_index timmissing20.json


def generate_packet(time, voltage, angle, settings={"pmu_measurement_bytes": 8, "destination_ip": "192.168.0.100", "destination_port": 4712}):
	# Define the PMU packet as a byte string
	datetime_str = str(time)[:26]

	try:
		dt = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
	except ValueError:
		dt = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

	# 2 byte
	sync = b'\xAA\x01'

	# 2 byte, 44 for 32 bit values of PMU, 40 for 16 bit values of PMU
	# 36 - 8 + 8 * number of PMUs || 36 - 8 + 4 * number PMUs
	frame_size = b'\x00\x24'

	# 2 byte, 12 for this
	id_code = b'\x00\x0C'

	# 4 byte
	soc = int(dt.strftime("%s")).to_bytes(4, 'big')
	print(dt.strftime("%s"))
	# 4 byte
	frac_sec = dt.microsecond.to_bytes(4, 'big')
	# 2 byte (no errors)
	stat = b'\x00\x00'

	# 4 or 8 byte x number of phasors (see doc, 8 is for float)
	voltage_bytes = struct.pack('>f', voltage)
	angle_bytes = struct.pack('>f', math.radians(angle))
	phasors = voltage_bytes + angle_bytes

	# 2 byte, assumed 60
	freq = b'\x09\xC4'

	# 2 byte
	dfreq = b'\x00\x00'

	# 4 byte
	analog = b'\x00\x00\x00\x00'

	# 2 byte
	digital = b'\x00\x00'

	# 2 byte
	chk = b'\x00\x00'

	pmu_packet = sync + frame_size + id_code + soc + frac_sec + \
		stat + phasors + freq + dfreq + analog + digital + chk

	# Set the destination IP address and port number
	destination_ip = settings["destination_ip"]
	destination_port = 4712

	# Create a UDP socket
	udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

	# Send the PMU packet to the destination IP address and port number
	udp_socket.sendto(pmu_packet, (destination_ip, destination_port))

	# Close the UDP socket
	udp_socket.close()

def send_end_packet(end_packet, destination_ip,destination_port):
	udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	destination_port = 4712
	udp_socket.sendto(end_packet.encode(), (destination_ip, destination_port))
	udp_socket.close()


if __name__ == "__main__":

	# parser = argparse.ArgumentParser(
						# prog='pmu-packet-sender',
						# description='Sends pmu packets',
						# epilog='Text at the bottom of help')
	# parser.add_argument('filename')
	# parser.add_argument('--ip', default="10.0.2.2")
	# parser.add_argument('--port', default=4712)
	# parser.add_argument('--num_packets', default=-1)
	# parser.add_argument('--drop_indexes', default='./evaluation/missing-data.json')

	# args = parser.parse_args()
	
	# args_filename = args.filename
	# args_ip = args.ip
	# args_port = args.port
	# args_num_packets = args.num_packets
	# args_drop_indexes = args.drop_indexes
	
	# python3 send.py pmu12.csv --ip 10.0.2.2 -- port 4712 --num_packets 100 --drop_index timmissing20.json
	args_filename = "pmu12.csv"
	args_ip = "10.0.2.2"
	args_port = 4712
	args_num_packets = 100
	args_drop_indexes = "timmissing20.json"
	
	f = open(args_drop_indexes)

	drop_indexes = json.load(f)

	pmu_data = parse_csv_data(
		args_filename,
		"TimeTag",
		["Magnitude01", "Magnitude02", "Magnitude03"],
		["Angle01", "Angle02", "Angle03"]
	)

	start_time = 0
	settings_obj ={"destination_ip": "", "destination_port": int(args_port)}
	#first 3 packets exists in switch
	for i in range(3, min(int(args_num_packets), len(pmu_data["times"]))):
		if i == 3:
			print(pmu_data["times"][i])

		#sending to loopback as opposed to switch
		settings_obj = {"destination_ip": "127.0.0.1" if i in drop_indexes else  args_ip, "destination_port": int(args_port)}

		print(str(i+1) + " | " + "Magnitude: " + str(pmu_data["magnitudes"][0][i]) + " | Phase_angle: " + str(pmu_data["phase_angles"][0][i]))
		time.sleep(0.017)
		generate_packet(pmu_data["times"][i], pmu_data["magnitudes"][0][i], pmu_data["phase_angles"][0][i], settings_obj)

		if i == 3 :
			start_time = time.time()

		# record timing in every round
	with open('sender_missing.txt','a') as f:
		f.write(f"Starting time: {start_time}\n")


	# send end packet
	end_packet = "END_OF_TRANSMISSION"
	destination_ip = settings_obj["destination_ip"]

	time.sleep(1)
	send_end_packet(end_packet, "127.0.0.1",4712)
	# generate_packets()
