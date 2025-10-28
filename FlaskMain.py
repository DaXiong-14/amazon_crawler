from flask import Flask, request, jsonify
import time
from queue import Queue
from threading import Thread

app = Flask(__name__)

# 任务队列和锁
task_queue = Queue()
current_task = None


def worker():
    global current_task
    while True:
        task = task_queue.get()  # 阻塞获取任务
        current_task = task
        try:
            print(f"开始执行任务: {task['id']}")
            # 模拟耗时操作
            time.sleep(5)
            print(f"任务 {task['id']} 完成")
        finally:
            current_task = None
            task_queue.task_done()


# 启动工作线程
Thread(target=worker, daemon=True).start()


@app.route('/api/crawler/submit', methods=['POST'])
def submit_task():
    data = request.json
    task_id = data.get('id', str(time.time()))

    # 将任务加入队列
    task_queue.put({'id': task_id, 'data': data})

    return jsonify({
        'status': 'queued',
        'task_id': task_id,
        'queue_size': task_queue.qsize()
    })


@app.route('/status')
def get_status():
    return jsonify({
        'current_task': current_task,
        'queue_size': task_queue.qsize()
    })


if __name__ == '__main__':
    app.run(port=5000)