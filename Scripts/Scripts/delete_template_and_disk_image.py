from cProfile import label
from calendar import c
from email import header
import os
from re import template
import re
import subprocess
from collections import  defaultdict
from urllib import request, response
import csv
project=os.environ['project']
Running_template_status=defaultdict(lambda:"STOP")
Running_disk_image_status=defaultdict(lambda:"STOP")
template_resource_info=defaultdict(list)
same_regex_templates=defaultdict(list)

#Running gcp command and returning the specfic output
def run_gcp_command(cmd):
    request=subprocess.check_output(cmd,shell=True)
    response=request.decode('utf-8').splitlines()
    response_list=response[1:]
    return response_list

def extract_request(cmd):
    try:
        request=subprocess.check_output(cmd,shell=True)
        response=request.decode('utf-8').splitlines()
        response=response[0].split('/')
        template=response[-1]
        return template

    except Exception as e:
        print(e)
        return

def is_response_valid(data):
    if len(data)>=1:
        return True
    return False

def filter_disk_from_template(cmd):
    try:
        response=subprocess.check_output(cmd,shell=True)

        if is_response_valid(response)==False:
            return []

        response_list=response.decode('utf-8').splitlines()[0].split(';')
        disk_image_list=[]
        for response in response_list:
            response=response.split('/')
            response=response[-1]
            disk_image_list.append(response)
        return disk_image_list

    except Exception as e:
        print(e)
        return []

def filter_templates_from_mig(cmd):
    try:
        response=subprocess.check_output(cmd,shell=True)

        if is_response_valid(response)==False:
            return []

        response_list=response.decode('utf-8').splitlines()[0].split(';')
        template_list=[]
        for response in response_list:
            response=response.split('/')
            response=response[-1]
            template_list.append(response)
        return template_list

    except Exception as e:
        print(e)
        return []


def template_creation_time(cmd):
    try:
        response=subprocess.check_output(cmd,shell=True)

        if is_response_valid(response)==False:
            return []
        response_list=response.decode('utf-8').splitlines()
        response_list=response_list[0]
        return response_list
    except Exception as e:
        print(e)

def check_template():

    #Finding all instance-templates
    cmd='''gcloud compute instance-templates list --project='''+project+''' | awk '{ print $1 }' '''
    template_list=run_gcp_command(cmd)




    #  print(template_resource_info)


    disk_image_list=[]
    for template in template_list:

        cmd='''gcloud compute instance-templates describe ''' +template+''' --project='''+project+''' --format='get(creationTimestamp)' '''
        creation_time=template_creation_time(cmd)
        template_resource_info[template].append(creation_time)

        template_regex=template.split('template')[0]
        same_regex_templates[template_regex+"template"].append(template)

        cmd='''gcloud compute instance-templates describe ''' +template+''' --project='''+project+''' --format='get(properties.disks.initializeParams.sourceImage)' '''
        disk_list=filter_disk_from_template(cmd)
        for disk_image in disk_list:
            disk_image_list.append(disk_image)
            template_resource_info[template].append(disk_image)
        cmd='''gcloud compute instance-templates describe ''' +template+''' --project='''+project+''' --format='get(properties.disks.initializeParams.sourceImage)' '''

    cmd='''gcloud compute instances list --project='''+project+''' | awk '{ print $2 }' '''
    zones_list=set(run_gcp_command(cmd))



    for zone in zones_list:

        #Finding all managed instace groups
        cmd='''gcloud compute instance-groups managed list --project='''+project+ ''' --zones='''+zone+ ''' | awk '{ print $1}' '''
        mig_list=run_gcp_command(cmd)

        for mig in mig_list:

            cmd='''gcloud compute instance-groups managed describe ''' + mig+''' --project='''+project+''' --zone='''+zone+''' --format='get(versions.instanceTemplate)' '''
            running_template_list=filter_templates_from_mig(cmd)

            for template in running_template_list:
                Running_template_status[template]="RUNNING"
                cmd='''gcloud compute instance-templates describe ''' +template+''' --project='''+project+''' --format='get(properties.disks.initializeParams.sourceImage)' '''
                disk_list=filter_disk_from_template(cmd)

                for disk_image in disk_list:
                    Running_disk_image_status[disk_image]="RUNNING"


    # with open('templates_list.csv', mode='w') as template_list_file:

    #     fieldnames = ['Template_Name', 'Status']
    #     writer = csv.DictWriter(template_list_file, fieldnames=fieldnames)
    #     writer.writeheader()

    #     for template in template_list:
    #         writer.writerow({'Template_Name': template, 'Status': Running_template_status[template]})

    # with open('disk_image_list.csv', mode='w') as disk_image_list_file:

    #     fieldnames = ['Disk_image_Name', 'Status']
    #     writer = csv.DictWriter(disk_image_list_file, fieldnames=fieldnames)
    #     writer.writeheader()

    #     for disk_image in disk_image_list:
    #         writer.writerow({'Disk_image_Name': disk_image, 'Status': Running_disk_image_status[disk_image]})



    for temp_list in same_regex_templates.values():
        list_of_items_to_delete=[]
        for template in temp_list:

            if Running_template_status[template]=="STOP":
                list=template_resource_info[template]
                list.append(template)
                list_of_items_to_delete.append(list)

        list_of_items_to_delete.sort(key=lambda x:x[0])
        print("intsance to be deleted")
        print(list_of_items_to_delete)
        list_of_items_to_delete=list_of_items_to_delete[10:]


        print("Sensitive region")
        for items in list_of_items_to_delete:
            length=len(items)

            #delete the disk images
            for disk_image in range(1,length-1):
                cmd='''gcloud compute images delete ''' + disk_image
                try:
                    response=subprocess.check_output(cmd,shell=True)
                    print("Disk image deleted successfully ",disk_image)
                except Exception as e:
                    print(e)



                # delete the template
                cmd='''gcloud compute instance-templates delete ''' + items[-1]
                try:
                    response=subprocess.check_output(cmd,shell=True)
                    print("Template deleted successfully ",items[-1])
                except Exception as e:
                    print(e)


check_template()













# for bucket in gcs_list:
#     bucket_name=bucket.split('/')[-2]
#     cmd='''gsutil label ch -l microservice:'''+bucket_name+" "+bucket
#     update_label_in_storage(cmd)
#     cmd='''gsutil label ch -l ms-resources:'''+bucket_name+"-gcs"+" "+bucket
#     update_label_in_storage(cmd)
