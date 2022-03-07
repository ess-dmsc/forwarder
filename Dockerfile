FROM python:3.7

ADD requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

ADD . .

CMD [ "bash", "./docker_launch_script.sh"]
