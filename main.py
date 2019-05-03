import boto3
import csv
import os
import datetime


def file_validator(all_file_data, first_row, file_name_local, file_name_aws, s3, bucketName):
    for data in all_file_data:
        if len(first_row) == len(data):
            print("data is matched with header")
            date_time_index = first_row.index("Date&Time")
            print(date_time_index)
            # x = datetime.datetime.now()
            # print(x)
            date_time = data[date_time_index]
            print(date_time)
            date_time = date_time.split(" ")
            date_time = list(filter(None, date_time))
            date = date_time[0]
            date_time.remove(date)
            time = str(date_time)
            header = []
            all_value = []
            with open(file_name_local, newline='') as infile:
                reader = csv.reader(infile)
                all_file_data = [row for row in reader]
                for head in all_file_data[0]:
                    if head != "Date&Time":
                        header.append(head)
                    elif head == "Date&Time":
                        header.append("Date")
                        header.append("Time")
                        index_for_date = all_file_data[0].index("Date&Time")
                    else:
                        pass
                all_value.append(header)
                for value in range(1,len(all_file_data)):
                    value_data = []
                    for val in range(0,len(all_file_data[value])):
                        if val != 5:
                            value_data.append(all_file_data[value][val])
                        elif val == 5:
                            value_data.append(date)
                            value_data.append(time)
                    all_value.append(value_data)
            with open("out"+file_name_local, 'w') as outfile:
                writer = csv.writer(outfile)
                writer.writerows(all_value)
            s3.meta.client.upload_file("out"+file_name_local, bucketName, file_name_aws)
            os.remove("out"+file_name_local)

        else:
            print("provided data not mapped with header!")
            s3.meta.client.upload_file(file_name_local, 'error8kmilesfile', file_name_aws)
            s3.meta.client.delete_object(Bucket=bucketName, Key=file_name_aws)
            break
    return


def download_file(bucket):
    aws_file_name = []
    local_file_name = []
    for obj in bucket.objects.all():
        file_name = obj.key
        aws_file_name.append(file_name)
        cli = 'client_'+file_name
        bucket.download_file(file_name, cli)
        local_file_name.append(cli)
    return (local_file_name, aws_file_name)


if __name__ == "__main__":
    access_key_id = 'xxx'
    secreat_access_key = 'xxx'
    sesssion = boto3.Session(access_key_id, secreat_access_key)
    s3 = sesssion.resource('s3')
    bucketName = '8kmilesfile'
    bucket = s3.Bucket(bucketName)

    file_name_local, file_name_aws = download_file(bucket)

    for ind in range(0, len(file_name_local)):
        with open(file_name_local[ind], newline='') as myFile:
            reader = csv.reader(myFile)
            all_file_data = [row for row in reader]
            first_row = all_file_data[0]
            first_row = list(filter(None, first_row))
            file_validator(all_file_data, first_row, file_name_local[ind], file_name_aws[ind], s3, bucketName)

        os.remove(file_name_local[ind])
