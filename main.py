import random
from queue import Queue

from flashtext import KeywordProcessor
from concurrent.futures import ThreadPoolExecutor

kp = KeywordProcessor()
with open('keywords.txt', 'r') as f:
    for word in f:
        kp.add_keyword(word.strip().lower())

thread_count = 10
request_queue=Queue()

def task():
    try:
        rand=random.Random()
        while True:
            with open('{}.txt'.format(rand.randint(1000,2000)), 'w') as f:

                docs = request_queue.get()
                if docs != None:
                    keywords_found = kp.extract_keywords(docs)
                    print(keywords_found)
                    f.write("\n".join(keywords_found))
                    f.flush()
                else:
                    break
    except Exception as e:
        print(e)
        raise e

executor=ThreadPoolExecutor(thread_count)
try:
    for i in range(thread_count):
        executor.submit(task)

    with open('docs.txt', 'r') as f:
        tmp = []
        for index, line in enumerate(f):
            tmp.append(line)
            if index % 500 == 0:

                request_queue.put('\n'.join(tmp))
                tmp.clear()
        if len(tmp) > 0 :
            request_queue.put('\n'.join(tmp))
            tmp.clear()
finally:
    for i in range(thread_count):
        request_queue.put(None)
    executor.shutdown()