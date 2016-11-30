#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import os
import Queue as queue
import sys
import thread

import pika


# Configurando o Logger do módulo
logging_level = logging.DEBUG

console_handler = logging.StreamHandler()
console_handler.setLevel(logging_level)

logger = logging.getLogger(__name__)
logger.setLevel(logging_level)
logger.addHandler(console_handler)

# Diretório completo dos dados da aplicação
data_path = "{1}{0}data{0}{2}"
# data_path = "{1}{0}data_testes{0}{2}" # Arquivos de testes, contêm 10 linhas
current_dir = os.getcwd()

friendships_path = data_path.format(os.sep, current_dir, "friendships.dat")
comments_path = data_path.format(os.sep, current_dir, "comments.dat")
likes_path = data_path.format(os.sep, current_dir, "likes.dat")
posts_path = data_path.format(os.sep, current_dir, "posts.dat")

# Tempo do último batch de eventos enviados ao serviço de filas
last_iteration = datetime.datetime.now()
timestamp_format = "%Y-%m-%dT%H:%M:%S.%f"

if len(sys.argv) == 3:
    time_speed_factor = int(sys.argv[2])
    ip_address = sys.argv[1]

else:
    # time_speed_factor = 86400   # 1d/s
    time_speed_factor = 43200   # 0.5d/s
    # time_speed_factor = 21600   # 0.25d/s

    # ip_address = '172.16.206.18'
    # ip_address = '192.168.25.7'
    # ip_address = '172.16.131.67'
    ip_address = 'localhost'

# Configuração do serviço de filas
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host=ip_address, port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
exchange_name = "amq.topic"
queue_name = "SOCIAL_NETWORK_EVENTS"

# Fila das mensagens de evento
message_queue = queue.PriorityQueue()

channel.exchange_declare(exchange=exchange_name, type='topic', durable=True)
channel.queue_declare(queue=queue_name) # StreamProcessing
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key="#")

try:
    with open(friendships_path, 'r') as friendships:
        friendships_init_time = friendships.readline().split('+')[0]

    with open(comments_path, 'r') as comments:
        comments_init_time = comments.readline().split('+')[0]

    with open(likes_path) as likes:
        likes_init_time = likes.readline().split('+')[0]

    with open(posts_path) as posts:
        posts_initial_time = posts.readline().split('+')[0]

    initial_timestamp = min(
        friendships_init_time,
        comments_init_time,
        likes_init_time,
        posts_initial_time
    )

    simulated_time = datetime.datetime.strptime(
        initial_timestamp, timestamp_format)

    logger.debug("SIMULATED TIME: %s", simulated_time)

except IOError as e:
    sys.stderr.write("ARQUIVO NÃO ENCONTRADO\n{}".format(str(e)))
    sys.exit(1)

# Indicadores para os arquivos foram lidos até o fim
friendships_is_closed = False
comments_is_closed = False
likes_is_closed = False
posts_is_closed = False


def get_datetime_from(string):
    return datetime.datetime.strptime(string, timestamp_format)


def mark_as_closed(filename):
    """
    Marca o arquivo como completamente lido.

    :param filename: Nome do arquivo a ser marcado como lido
    :return: None
    """
    global friendships_is_closed
    global comments_is_closed
    global likes_is_closed
    global posts_is_closed

    if filename == 'friendships':
        friendships_is_closed = True
    elif filename == 'comments':
        comments_is_closed = True
    elif filename == 'likes':
        likes_is_closed = True
    elif filename == 'posts':
        posts_is_closed = True


def has_open_files():
    """
    Verifica se ainda existem arquivos abertos.

    :return: True caso existam arquivos em aberto, False caso todos estejam
    fechados.
    """
    return (friendships_is_closed or
            comments_is_closed or
            likes_is_closed or
            posts_is_closed)


def parse_events(file_path):
    """
    Lê o arquivo e insere as linhas na lista de eventos.

    :param file_path: Diretório completo do arquivo.
    :return: None
    """
    filename = file_path.split(os.sep)[-1]
    event_topic = filename.strip('.dat')

    input_file = open(file_path, 'r')
    line_read = input_file.readline().strip('\n')

    while line_read != '':
        timestamp = line_read.split('+')[0]

        message_queue.put_nowait(
            (timestamp, event_topic, line_read.strip('\n')))

        line_read = input_file.readline()

    mark_as_closed(filename)
    input_file.close()


def send_to_queue_service():
    """
    Publica no serviço de filas os eventos armazenados baseado no tempo simulado
    de execução.

    :return: None
    """
    global last_iteration
    global simulated_time

    while True:
        time_now = datetime.datetime.now()
        elapsed_seconds = (time_now - last_iteration).total_seconds()

        if elapsed_seconds < 1:
            continue

        last_iteration = time_now
        simulated_time += datetime.timedelta(seconds=1 * time_speed_factor)

        while not message_queue.empty():
            event = message_queue.get_nowait()
            timestamp = event[0]
            event_topic = event[1]
            message = event[2]

            event_time = get_datetime_from(timestamp)
            time_to_next = (event_time - simulated_time).total_seconds()

            if time_to_next > 0:
                message_queue.put_nowait(event)
                logger.debug(
                    "NO EVENT -- REMAINING EVENTS: %s", message_queue.qsize())
                break

            logger.debug(
                "SENT: (Topic: %s, Message: %s)", event_topic[:5], message)

            channel.basic_publish(
                exchange=exchange_name,
                routing_key=event_topic,
                body=message
            )

        if (not has_open_files()) and message_queue.empty():
            break


if __name__ == "__main__":
    logger.info("--INICIANDO LEITURA--")

    try:
        friendship_reader = thread.start_new_thread(
            parse_events, (friendships_path,))

        post_reader = thread.start_new_thread(
            parse_events, (posts_path,))

        comment = thread.start_new_thread(
            parse_events, (comments_path,))

        like = thread.start_new_thread(
            parse_events, (likes_path,))

        logger.info("Enviando mensagens...")
        send_to_queue_service()
        logger.info("Todas as mensagens foram enviadas")

    except KeyboardInterrupt:
        logger.info("--LEITURA INTERROMPIDA--")
        sys.exit(1)
