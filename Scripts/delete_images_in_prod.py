from cProfile import label
from calendar import c
from email import header
from email.mime import image
import os
from re import template
import re
import subprocess
from collections import  defaultdict
from urllib import request, response
import csv
from datetime import timedelta,datetime
from dateutil import parser
project=os.environ['project']
import logging
last_recently_used_images_count=10
Running_disk_image_status=defaultdict(lambda:"STOP")
creation_time_of_disk=defaultdict(lambda:"NOT_FOUND")
service_vice_disk_image_list=defaultdict(list)



####Logging Agent ###########
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='template_delete.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.info("logging starts")
##################################

#Running gcp command and returning the specfic output
def run_gcp_command(cmd):
    request=subprocess.check_output(cmd,shell=True)
    response=request.decode('utf-8').splitlines()
    response_list=response[1:]
    return response_list

###extracting the request in format##############
def extract_request(cmd):
    try:
        request=subprocess.check_output(cmd,shell=True)
        response=request.decode('utf-8').splitlines()
        response=response[0].split('/')
        disk=response[-1]
        return disk

    except Exception as e:
        print(e)
        return

    ###checking for valid response ##############
def is_response_valid(data):
    if len(data)>=1:
        return True
    return False


#####Extracting lablel from a service ##############
def extract_label(cmd):
    response=subprocess.check_output(cmd,shell=True)
    response=response.decode('utf-8').splitlines()
    label=response[0]
    print(label)
    return label

###Extracting disk creaation Time######################
def disk_creation_time(cmd):
    try:
        response=subprocess.check_output(cmd,shell=True)

        if is_response_valid(response)==False:
            return []
        response_list=response.decode('utf-8').splitlines()
        response_list=response_list[0]
        return response_list
    except Exception as e:
        print(e)
        return " "

####Checking time########################
def get_time_check(string_time):
    #replace the last ':' with an empty string, as python UTC offset format is +HHMM
    strs = string_time[::-1].replace(':','',1)[::-1]
    offset=0
    try:
        offset = int(strs[-5:])
    except:
        print("Error")

    delta = timedelta(hours = offset / 100)
    time_creation = datetime.strptime(strs[:-5], "%Y-%m-%dT%H:%M:%S.%f")
    print(time_creation)
    time_creation -= delta                #reduce the delta from this time object
    time_now=datetime.now()
    print(time_now)
    return (time_now-time_creation).days







def check_disk_images():

    #Finding all instance-templates
    cmd='''gcloud compute disks list --project='''+project+''' | awk '{ print $1 }' '''
    disks_list=run_gcp_command(cmd)
    cmd='''gcloud compute disks list --project='''+project+''' | awk '{ print $2 }' '''
    zones_list=run_gcp_command(cmd)



    #  print(template_resource_info)

    iter=0
    disk_image_list=[]
    iter=0
    print(disks_list)
    for disk in disks_list:
        print(disk)
        cmd='''gcloud compute disks describe ''' +disk+''' --project='''+project+''' --format='get(sourceImage)'  --zone='''+zones_list[iter]
        iter+=1
        disk_image=extract_request(cmd)
        status="RUNNING "+disk + " is using this"
        Running_disk_image_status[disk_image]=status


    cmd='''gcloud compute images list --project='''+project+''' | awk '{ print $1 }' '''

    disk_image_list=run_gcp_command(cmd)
    print(disk_image_list)


    non_running_disk_image_list=[]

    for disk_image in disk_image_list:
        if Running_disk_image_status[disk_image]=='STOP':
            non_running_disk_image_list.append(disk_image)


    non_deployed_disk_images_list=[]
    images_to_delete=[]

    ###Finding the deployed disks ###############
    for disk_image in non_running_disk_image_list:
        image_name=disk_image
        cmd='''gcloud compute images describe ''' +disk_image+''' --project='''+project+''' --format='get(creationTimestamp)' '''
        creation_time=disk_creation_time(cmd)

        print(creation_time)
        if creation_time==" ":
            print("Creation time doesn't found for the disk image ",disk_image)
            continue
        creation_time_of_disk[image_name]=creation_time
        if image_name.find('deployed')!=-1:
            cmd='''gcloud compute images describe  ''' + image_name+''' --format='get(labels.service)' '''
            service_tag=extract_label(cmd)
            if service_tag==" ":
                print("service label not found for the disk_image ",disk_image)
                continue;
            service_vice_disk_image_list[service_tag].append(disk_image)



    print(service_vice_disk_image_list)
    ###########Extracting all the disks to delete #################
    for service_disk_list in service_vice_disk_image_list.values():
        list_of_items=[]
        print("first loop")
        print(service_disk_list)

        for disk_image in service_disk_list:
            list=[]
            print("in for loop")
            print(disk_image)
            print(creation_time_of_disk[disk_image])

            list.append(creation_time_of_disk[disk_image])
            list.append(disk_image)
            list_of_items.append(list)

        list_of_items.sort(key=lambda x:x[0])
        length=len(list_of_items)
        print(length)
        print(list_of_items)
        for iter in range(last_recently_used_images_count,length):
            print(get_time_check(list_of_items[iter][0]))
            if get_time_check(list_of_items[iter][0])>30:
                images_to_delete.append(list_of_items[iter][1])

    print("printing the images to delete",images_to_delete)
    for image in images_to_delete:
        cmd='''gcloud compute images delete '''+image
        run_gcp_command(cmd)

check_disk_images()
