import asyncio, ssl, logging, os
import aiomqtt

def setup_logger():
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
        level=logging.INFO,
        datefmt='%d/%m/%Y %H:%M:%S %z'
    )

async def dispatcher(client, topics):
    task_name = asyncio.current_task().get_name()
    logger = logging.getLogger(task_name)

    # Suscribirse a todos los tópicos
    for topic in topics:
        await client.subscribe(topic)
        logger.info(f"Suscrito al tópico: {topic}")

    async for message in client.messages:
        topic = message.topic.value
        payload = message.payload.decode()

        # Filtrado simple por tópico
        if topic == topics[0]:
            logger.info(f"[{topic}] Temperatura: {payload}")
        elif topic == topics[1]:
            logger.info(f"[{topic}] Humedad: {payload}")
        else:
            logger.warning(f"Mensaje recibido en tópico no esperado: {topic}")

async def incrementar_contador(counter_queue):
    task_name = asyncio.current_task().get_name()
    logger = logging.getLogger(task_name)

    contador = 0
    while True:
        await asyncio.sleep(3)
        contador += 1
        await counter_queue.put(contador)
        logger.info(f"Contador incrementado a {contador}")

async def publicar_contador(client, counter_queue, topic_pub):
    task_name = asyncio.current_task().get_name()
    logger = logging.getLogger(task_name)

    while True:
        await asyncio.sleep(5)
        if not counter_queue.empty():
            contador = await counter_queue.get()
            await client.publish(topic_pub, str(contador))
            logger.info(f"Publicado {contador} en {topic_pub}")

async def main():
    setup_logger()

    tls_context = ssl.create_default_context()
    tls_context.check_hostname = True
    tls_context.verify_mode = ssl.CERT_REQUIRED

    async with aiomqtt.Client(
        os.environ['SERVIDOR'],
        port=8883,
        tls_context=tls_context,
    ) as client:
        topic1 = os.environ['TOPICO1']
        topic2 = os.environ['TOPICO2']
        topic_pub = os.environ['TOPICO_PUB']

        counter_queue = asyncio.Queue()

        tasks = [
            asyncio.create_task(dispatcher(client, [topic1, topic2]), name="Task"),
            asyncio.create_task(incrementar_contador(counter_queue), name="Incrementador"),
            asyncio.create_task(publicar_contador(client, counter_queue, topic_pub), name="Publicador"),
        ]

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Apagando por Ctrl+C...")