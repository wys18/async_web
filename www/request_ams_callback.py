import requests
import time
import random

url = 'https://ai-api.sensoro.com/app/alert/recovery'
bucket = 'pek-sensoro-camera'
sns = (44444444, 55555555, 88888888)
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
           'Accept-Language': 'en-US,en;q=0.6'}


def file_callback(sn):
    package_key = 'alarm/{}/detect/VDA_1534490349.tgz'.format(sn)
    video_key = 'alarm/{}/detect/VDA_1534490349.tgz'.format(sn)
    print(package_key)
    print(video_key)
    data = dict(bucket=bucket, videoKey=video_key, packageKey=package_key, isUploadCloud=0)
    resu = requests.post(url, json=data, headers=headers)
    print(resu.content.decode())

#
# def send():
#     while True:
#         sn = random.sample(sns, 1)
#         sn = sn[0]
#         file_callback(sn, 'video', 'h265')
#         file_callback(sn, 'detect', 'bin')
#         time.sleep(3600)


def send_test(sn):
    file_callback(sn)


if __name__ == '__main__':
    send_test(22222222)
